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

	"github.com/cenkalti/backoff/v5"
	"cloud.google.com/go/pubsub"
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

	minHold := &atomic.Int64{}
	minHold.Store(int64(5 * time.Minute))
	checkInt := &atomic.Int64{}
	checkInt.Store(int64(1 * time.Minute))

	config := ListenerConfig{
		Client:            podPSClient,
		TopicID:           topicID,
		TopicProject:      topicProject,
		TopicName:         topicName,
		Topic:             topic,
		SubID:             subID,
		MinHoldDuration:   minHold,
		MinHoldDurationMu: &sync.Mutex{},
		CheckInterval:     checkInt,
		CheckIntervalMu:   &sync.Mutex{},
	}

	l := &TopicListener{
		updateConfig: config.UpdateConfig,
		stateCh:      make(chan bool, 1),
		recreateCh:   make(chan struct{}, 1),
		stopCh:       make(chan struct{}),
	}

	go l.listen(config)

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

	if l.updateConfig(holdDuration, checkInterval) {
		select {
		case l.recreateCh <- struct{}{}:
		default:
		}
	}
}

func (l *TopicListener) unregister(notifyCh chan bool) {
	l.notifyChannels.Delete(notifyCh)
}

func (l *TopicListener) broadcastLoop() {
	for {
		select {
		case <-l.stopCh:
			return
		case active := <-l.stateCh:
			l.notifyChannels.Range(func(key, value interface{}) bool {
				ch := key.(chan bool)
				select {
				case ch <- active:
				default:
				}
				return true
			})
		}
	}
}

func (l *TopicListener) listen(config ListenerConfig) {
	log.Printf("Starting listener for topic %s on sub %s", config.TopicID, config.SubID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go l.broadcastLoop()

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

		// Allow recreate by canceling the current operation
		go func() {
			select {
			case <-l.recreateCh:
				opCancel()
			case <-opCtx.Done():
			}
		}()

		sub, err := config.Client.CreateSubscription(opCtx, config.SubID, pubsub.SubscriptionConfig{
			Topic:            config.Topic,
			ExpirationPolicy: 24 * time.Hour,
		})
		if st, _ := status.FromError(err); st.Code() == codes.AlreadyExists {
			sub = config.Client.Subscription(config.SubID)
		} else if err != nil {
			log.Printf("Warning: failed to create subscription %s, retrying: %v", config.SubID, err)
			return struct{}{}, err
		}

		op := &receiveOperation{
			config:  config,
			sub:     sub,
			stateCh: l.stateCh,
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
		op.stateMu.Lock()
		if op.holdTimer != nil {
			op.holdTimer.Stop()
			op.holdTimer = nil
		}
		op.stateMu.Unlock()
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
	if !op.active {
		op.active = true
		select {
		case op.stateCh <- true:
		case <-c.Done():
		}
		log.Printf("Topic %s ACTIVE", op.config.TopicID)
	}

	op.lease++
	currentLease := op.lease

	if op.holdTimer != nil {
		op.holdTimer.Stop()
	}

	op.holdTimer = time.AfterFunc(hold, func() {
		op.stateMu.Lock()
		defer op.stateMu.Unlock()

		if op.active && op.lease == currentLease {
			op.active = false
			op.stateCh <- false
			log.Printf("Topic %s INACTIVE (idle for %s)", op.config.TopicID, hold)
		}
	})
	op.stateMu.Unlock()

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
