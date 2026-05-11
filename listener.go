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
		config:   config,
		stateCh:  make(chan bool, 1),
		stopCh:   make(chan struct{}),
		opCtx:    context.Background(),
		opCancel: func() {},
	}

	go l.broadcastLoop()
	go l.ensureSubscription()

	return l, nil
}

func (l *TopicListener) ensureSubscription() {
	if l.subReady.Load() {
		return
	}

	operation := func() (struct{}, error) {
		_, err := l.config.Client.CreateSubscription(context.Background(), l.config.SubID, pubsub.SubscriptionConfig{
			Topic:            l.config.Topic,
			ExpirationPolicy: 24 * time.Hour,
		})
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.AlreadyExists {
				// exists
			} else {
				log.Printf("Warning: failed to create subscription %s, retrying: %v", l.config.SubID, err)
				return struct{}{}, err
			}
		}

		l.subReady.Store(true)

		l.streamMu.Lock()
		if l.streamCount > 0 {
			l.startOperation()
		}
		l.streamMu.Unlock()

		return struct{}{}, nil
	}

	_, _ = backoff.Retry(context.Background(), operation, backoff.WithBackOff(backoff.NewExponentialBackOff()))
}

func (l *TopicListener) getSubscription() (*pubsub.Subscription, error) {
	if !l.subReady.Load() {
		return nil, fmt.Errorf("subscription not yet created")
	}
	return l.config.Client.Subscription(l.config.SubID), nil
}

func (l *TopicListener) startOperation() {
	sub, err := l.getSubscription()
	if err != nil {
		return
	}

	l.opCancel()
	l.opCtx, l.opCancel = context.WithCancel(context.Background())

	timer := time.NewTimer(0)
	timer.Stop()

	op := &receiveOperation{
		sub:             sub,
		topicID:         l.config.TopicID,
		minHoldDuration: l.config.MinHoldDuration,
		checkInterval:   l.config.CheckInterval,
		stateCh:         l.stateCh,
		onNotFound: func() {
			l.subReady.Store(false)
			go l.ensureSubscription()
		},
		holdTimer: timer,
	}

	l.activeOp.Store(op)
	go op.runWithBackoff(l.opCtx)
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
	l.streamMu.Lock()
	defer l.streamMu.Unlock()

	l.notifyChannels.Store(notifyCh, struct{}{})

	needsRecreate := l.config.UpdateConfig(holdDuration, checkInterval)

	if l.streamCount == 0 || needsRecreate {
		l.startOperation()
	}
	l.streamCount++

	// Immediately notify the new observer of the current state
	notifyCh <- l.IsActive()
}

func (l *TopicListener) unregister(notifyCh chan bool) {
	l.streamMu.Lock()
	defer l.streamMu.Unlock()

	l.notifyChannels.Delete(notifyCh)
	l.streamCount--
	if l.streamCount == 0 {
		l.opCancel()
	}
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

func (op *receiveOperation) runWithBackoff(ctx context.Context) {
	operation := func() (struct{}, error) {
		return op.run(ctx)
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

	interval := time.Duration(op.checkInterval.Load())
	op.sub.ReceiveSettings.MaxOutstandingMessages = 1
	op.sub.ReceiveSettings.NumGoroutines = 1
	op.sub.ReceiveSettings.Synchronous = true
	op.sub.ReceiveSettings.MaxExtension = interval + 1*time.Minute

	err := op.sub.Receive(ctx, op.processMessage)

	if st, _ := status.FromError(err); st.Code() == codes.NotFound {
		log.Printf("Topic or subscription not found for %s, marking not ready: %v", op.topicID, err)
		if op.onNotFound != nil {
			op.onNotFound()
		}
		return struct{}{}, backoff.Permanent(err)
	} else if err != nil {
		log.Printf("Receive error for topic %s: %v, retrying...", op.topicID, err)
	}
	return struct{}{}, err
}

func (op *receiveOperation) processMessage(c context.Context, msg *pubsub.Message) {
	hold := time.Duration(op.minHoldDuration.Load())

	op.stateMu.Lock()
	if !op.active {
		op.active = true
		select {
		case op.stateCh <- true:
		case <-c.Done():
		}
		log.Printf("Topic %s ACTIVE", op.topicID)
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
			log.Printf("Topic %s INACTIVE (idle for %s)", op.topicID, hold)
		}
	})
	op.stateMu.Unlock()

	// Hold the message for the check interval to prevent a tight Nack loop
	currentInterval := time.Duration(op.checkInterval.Load())
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
		log.Printf("Error purging sub %s: %v", op.sub.ID(), err)
	}
}
