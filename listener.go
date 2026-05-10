package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/cenkalti/backoff/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewTopicListener(podPSClient *pubsub.Client, topicID string) (*TopicListener, error) {
	topicParts := splitGCPResource(topicID)
	if len(topicParts) != 4 {
		return nil, fmt.Errorf("topic ID must be in the format 'projects/<project>/topics/<topic>', got: %s", topicID)
	}
	topicProject := topicParts[1]
	topicName := topicParts[3]

	h := fnv.New32a()
	podName, _ := os.Hostname()
	h.Write([]byte(topicID + podName))
	subID := fmt.Sprintf("keda-%s-%x", topicName, h.Sum32())

	topic := podPSClient.TopicInProject(topicName, topicProject)

	config := ListenerConfig{
		Client:            podPSClient,
		TopicID:           topicID,
		TopicProject:      topicProject,
		TopicName:         topicName,
		Topic:             topic,
		SubID:             subID,
		MinHoldDuration:   &atomic.Int64{},
		MinHoldDurationMu: &sync.Mutex{},
		CheckInterval:     &atomic.Int64{},
		CheckIntervalMu:   &sync.Mutex{},
	}

	config.MinHoldDuration.Store(int64(5 * time.Minute))
	config.CheckInterval.Store(int64(1 * time.Minute)) // Default to 1min

	l := &TopicListener{
		config: config,
		stopCh: make(chan struct{}),
	}

	go l.listen()

	return l, nil
}

func (l *TopicListener) IsActive() bool {
	op := l.activeOp.Load()
	if op != nil {
		return op.IsActive()
	}
	return false
}

func (s *PubSubScaler) getListener(topicID string) (*TopicListener, error) {
	s.listenersMu.RLock()
	if l, ok := s.listeners[topicID]; ok {
		s.listenersMu.RUnlock()
		return l, nil
	}
	s.listenersMu.RUnlock()

	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()

	// Double-check after acquiring write lock
	if l, ok := s.listeners[topicID]; ok {
		return l, nil
	}

	l, err := NewTopicListener(s.podPSClient, topicID)
	if err != nil {
		return nil, err
	}

	s.listeners[topicID] = l
	return l, nil
}

func (l *TopicListener) register(notifyCh chan bool, holdDuration time.Duration, checkInterval time.Duration) {
	l.notifyChannels.Store(notifyCh, struct{}{})

	l.config.UpdateHoldDuration(holdDuration)
	l.config.UpdateCheckInterval(checkInterval)

	// Immediately notify the new observer of the current state
	notifyCh <- l.IsActive()
}

func (l *TopicListener) unregister(notifyCh chan bool) {
	l.notifyChannels.Delete(notifyCh)
}

func (l *TopicListener) broadcast(active bool) {
	l.notifyChannels.Range(func(key, value interface{}) bool {
		ch := key.(chan bool)
		select {
		case ch <- active:
		default:
		}
		return true
	})
}

func (l *TopicListener) listen() {
	log.Printf("Starting listener for topic %s on sub %s", l.config.TopicID, l.config.SubID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-l.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	operation := func() (struct{}, error) {
		opCtx, opCancel := context.WithCancel(ctx)
		defer opCancel()

		sub, err := l.config.Client.CreateSubscription(opCtx, l.config.SubID, pubsub.SubscriptionConfig{
			Topic:            l.config.Topic,
			ExpirationPolicy: 24 * time.Hour,
		})
		if st, _ := status.FromError(err); st.Code() == codes.AlreadyExists {
			sub = l.config.Client.Subscription(l.config.SubID)
		} else if err != nil {
			log.Printf("Warning: failed to create subscription %s, retrying: %v", l.config.SubID, err)
			return struct{}{}, err
		}

		op := &receiveOperation{
			config:    l.config,
			sub:       sub,
			broadcast: l.broadcast,
		}

		l.activeOp.Store(op)

		return op.run(opCtx)
	}

	_, _ = backoff.Retry(ctx, operation, backoff.WithBackOff(backoff.NewExponentialBackOff()))
}

func (op *receiveOperation) IsActive() bool {
	op.stateMu.RLock()
	defer op.stateMu.RUnlock()
	return op.active
}

func (op *receiveOperation) run(ctx context.Context) (struct{}, error) {
	// Cleanup on error/exit
	defer func() {
		op.mu.Lock()
		if op.holdTimer != nil {
			op.holdTimer.Stop()
			op.holdTimer = nil
		}
		op.mu.Unlock()
	}()

	interval := time.Duration(op.config.CheckInterval.Load())
	op.sub.ReceiveSettings.MaxOutstandingMessages = 1
	op.sub.ReceiveSettings.NumGoroutines = 1
	op.sub.ReceiveSettings.Synchronous = true
	op.sub.ReceiveSettings.MaxExtension = interval + 1*time.Minute

	err := op.sub.Receive(ctx, op.processMessage)

	if st, _ := status.FromError(err); st.Code() == codes.NotFound {
		log.Printf("Topic or subscription not found for %s, will recreate and retry: %v", op.config.TopicID, err)
	} else if err != nil {
		log.Printf("Receive error for topic %s: %v, retrying...", op.config.TopicID, err)
	}
	return struct{}{}, err
}

func (op *receiveOperation) processMessage(c context.Context, msg *pubsub.Message) {
	hold := time.Duration(op.config.MinHoldDuration.Load())

	op.stateMu.Lock()
	op.lastMsgTime = time.Now()
	if !op.active {
		op.active = true
		op.broadcast(true)
		log.Printf("Topic %s ACTIVE", op.config.TopicID)
	}
	op.stateMu.Unlock()

	op.mu.Lock()
	// Cancel previous expiry trigger if it exists
	if op.holdTimer != nil {
		op.holdTimer.Stop()
	}

	// Spawn a new trigger for the expiry
	op.holdTimer = time.AfterFunc(hold, func() {
		op.stateMu.Lock()
		defer op.stateMu.Unlock()

		if op.active && time.Since(op.lastMsgTime) >= hold {
			op.active = false
			op.broadcast(false)
			log.Printf("Topic %s INACTIVE (idle for %s)", op.config.TopicID, hold)
		}
	})
	op.mu.Unlock()

	// Hold the message for the check interval to prevent a tight Nack loop
	currentInterval := time.Duration(op.config.CheckInterval.Load())
	select {
	case <-time.After(currentInterval):
	case <-c.Done():
	}

	// Purge the backlog right before Nacking. This clears the client buffer
	// and ensures the next poll is for fresh activity.
	op.purge()
	msg.Nack()
}

func (op *receiveOperation) purge() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := op.sub.SeekToTime(ctx, time.Now()); err != nil {
		log.Printf("Error purging sub %s: %v", op.config.SubID, err)
	}
}
