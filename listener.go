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
	h.Write([]byte(podName))
	subID := fmt.Sprintf("keda-%s-%x", topicName, h.Sum32())

	topic := podPSClient.TopicInProject(topicName, topicProject)

	minDebounce := &atomic.Int64{}
	minDebounce.Store(int64(5 * time.Minute))
	checkInt := &atomic.Int64{}
	checkInt.Store(int64(1 * time.Minute))

	config := ListenerConfig{
		Client:              podPSClient,
		Topic:               topic,
		SubID:               subID,
		MinDebounceDuration: minDebounce,
		CheckInterval:       checkInt,
		ConfigMu:            &sync.Mutex{},
	}

	ctx := context.Background()

	l := &TopicListener{
		updateConfig: config.UpdateConfig,
		reconcileCh:  make(chan bool, 1),
		runCtx:       ctx,
	}

	go l.controlLoop(config)

	return l, nil
}

func (l *TopicListener) ensureSubscription(config ListenerConfig) error {
	_, err := config.Client.CreateSubscription(l.runCtx, config.SubID, pubsub.SubscriptionConfig{
		Topic:            config.Topic,
		ExpirationPolicy: 24 * time.Hour,
	})
	if st, _ := status.FromError(err); st.Code() == codes.AlreadyExists {
		log.Printf("Verified subscription %s exists", config.SubID)
		return nil
	} else if err != nil {
		log.Printf("Warning: failed to create subscription %s, retrying: %v", config.SubID, err)
	} else {
		log.Printf("Successfully created subscription %s", config.SubID)
	}
	return err
}

func (l *TopicListener) setActive(active bool) {
	if active == l.isActive.Load() {
		return
	}
	l.isActive.Store(active)
	l.notifyChannels.Range(func(key, value interface{}) bool {
		ch := key.(chan bool)
		TrySend(ch, active)
		return true
	})
}

func (l *TopicListener) controlLoop(config ListenerConfig) {
	messageTick := make(chan struct{})
	debounceTimer := time.NewTimer(time.Hour)
	debounceTimer.Stop()

	for {
		if l.runCtx.Err() != nil {
			log.Printf("TopicListener control loop exiting: %v", l.runCtx.Err())
			return
		}

		operation := func() (struct{}, error) {
			return struct{}{}, l.ensureSubscription(config)
		}

		_, err := backoff.Retry(l.runCtx, operation, backoff.WithBackOff(backoff.NewExponentialBackOff()))
		if err != nil {
			log.Printf("TopicListener control loop exiting due to backoff error: %v", err)
			return // runCtx canceled
		}

		opCtx, opCancel := context.WithCancel(context.Background())
		opCancel() // start canceled
		opDoneCh := make(chan struct{})
		close(opDoneCh)

		stopOperation := func() {
			opCancel()
			<-opDoneCh
		}
		startOperation := func() {
			stopOperation()
			opCtx, opCancel = context.WithCancel(l.runCtx)
			opDoneCh = make(chan struct{})

			op := &receiveOperation{
				sub:                 config.Client.Subscription(config.SubID),
				minDebounceDuration: config.MinDebounceDuration,
				checkInterval:       time.Duration(config.CheckInterval.Load()),
				messageTick:         messageTick,
				runCtx:              opCtx,
			}

			go func() {
				defer close(opDoneCh)
				_, _ = backoff.Retry(opCtx, op.Run, backoff.WithBackOff(backoff.NewExponentialBackOff()))
			}()
		}

		// Trigger initial reconcile
		TrySend(l.reconcileCh, false)

	InnerLoop:
		for {
			select {
			case <-l.runCtx.Done():
				stopOperation()
				return
			case needsRecreate := <-l.reconcileCh:
				count := l.streamCount.Load()

				if needsRecreate || (count > 0 && opCtx.Err() != nil) {
					startOperation()
				} else if count == 0 {
					stopOperation()
				}

			case <-messageTick:
				l.setActive(true)
				debounceTimer.Reset(time.Duration(config.MinDebounceDuration.Load()))

			case <-debounceTimer.C:
				l.setActive(false)

			case <-opDoneCh:
				// Operation died (e.g. NotFound). Break inner loop to re-ensure subscription.
				stopOperation()
				break InnerLoop
			}
		}
	}
}

func (l *TopicListener) IsActive() bool {
	return l.isActive.Load()
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

func (l *TopicListener) Register(notifyCh chan bool, holdDuration time.Duration, checkInterval time.Duration) {
	l.notifyChannels.Store(notifyCh, struct{}{})

	needsRecreate := l.updateConfig(holdDuration, checkInterval)
	l.streamCount.Add(1)

	if needsRecreate {
		l.reconcileCh <- true
	} else {
		TrySend(l.reconcileCh, false)
	}
}

func (l *TopicListener) Unregister(notifyCh chan bool) {
	l.notifyChannels.Delete(notifyCh)
	l.streamCount.Add(-1)

	TrySend(l.reconcileCh, false)
}

func (op *receiveOperation) Run() (struct{}, error) {
	log.Printf("Starting stream pull for subscription %s", op.sub.ID())
	defer log.Printf("Stream pull for subscription %s stopped", op.sub.ID())

	op.sub.ReceiveSettings.MaxOutstandingMessages = 1
	op.sub.ReceiveSettings.NumGoroutines = 1
	op.sub.ReceiveSettings.Synchronous = true
	op.sub.ReceiveSettings.MaxExtension = op.checkInterval + 1*time.Minute

	return struct{}{}, op.sub.Receive(op.runCtx, op.processMessage)
}

func (op *receiveOperation) processMessage(c context.Context, msg *pubsub.Message) {
	log.Printf("Received message %s on subscription %s", msg.ID, op.sub.ID())

	select {
	case op.messageTick <- struct{}{}:
	case <-op.runCtx.Done():
	}

	// Hold the message for the check interval to prevent a tight Nack loop
	select {
	case <-time.After(op.checkInterval):
	case <-c.Done():
	}

	// Purge the backlog right before Nacking. This clears the client buffer
	// and ensures the next poll is for fresh activity.
	op.purge()
	msg.Nack()
	log.Printf("Nacked message %s on subscription %s", msg.ID, op.sub.ID())
}

func (op *receiveOperation) purge() {
	log.Printf("Purging backlog for subscription %s", op.sub.ID())
	// The shared runCtx handles scaler teardown, plus a timeout so SeekToTime doesn't block indefinitely
	ctx, cancel := context.WithTimeout(op.runCtx, 5*time.Second)
	defer cancel()
	if err := op.sub.SeekToTime(ctx, time.Now()); err != nil {
		log.Printf("Error purging sub %s: %v", op.sub.ID(), err)
	}
}
