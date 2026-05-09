package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"time"

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

	l := &TopicListener{
		podPSClient:  podPSClient,
		topicID:      topicID,
		topicProject: topicProject,
		topicName:    topicName,
		stopCh:       make(chan struct{}),
	}
	l.minHoldDuration.Store(int64(5 * time.Minute))
	l.checkInterval.Store(int64(1 * time.Minute)) // Default to 1min

	h := fnv.New32a()
	podName, _ := os.Hostname()
	h.Write([]byte(topicID + podName))
	subID := fmt.Sprintf("keda-%s-%x", topicName, h.Sum32())

	ctx := context.Background()
	topic := podPSClient.TopicInProject(topicName, topicProject)
	sub, err := podPSClient.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:            topic,
		ExpirationPolicy: 24 * time.Hour,
	})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.AlreadyExists {
			log.Printf("Warning: failed to create subscription %s, falling back to existing: %v", subID, err)
		}
		sub = podPSClient.Subscription(subID)
	}
	l.sub = sub

	// Active is initially false until a message arrives
	l.active = false

	go l.listen()

	return l, nil
}

func (l *TopicListener) IsActive() bool {
	l.stateMu.RLock()
	defer l.stateMu.RUnlock()
	return l.active
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

	// Ensure we use the minimum requested duration across all observers,
	// but never go below an absolute floor of 30 seconds.
	l.minHoldDurationMu.Lock()
	currentMin := time.Duration(l.minHoldDuration.Load())
	effectiveHold := max(30*time.Second, holdDuration)
	l.minHoldDuration.Store(int64(min(currentMin, effectiveHold)))
	l.minHoldDurationMu.Unlock()

	l.checkIntervalMu.Lock()
	currentCheck := time.Duration(l.checkInterval.Load())
	l.checkInterval.Store(int64(min(currentCheck, checkInterval)))
	l.checkIntervalMu.Unlock()

	// Immediately notify the new observer of the current state
	notifyCh <- l.IsActive()
}

func (l *TopicListener) unregister(notifyCh chan bool) {
	l.notifyChannels.Delete(notifyCh)
}

func (l *TopicListener) listen() {
	interval := time.Duration(l.checkInterval.Load())

	l.sub.ReceiveSettings.MaxOutstandingMessages = 1
	l.sub.ReceiveSettings.NumGoroutines = 1
	l.sub.ReceiveSettings.Synchronous = true
	l.sub.ReceiveSettings.MaxExtension = interval + 1*time.Minute

	log.Printf("Starting listener for topic %s on sub %s", l.topicID, l.sub.ID())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := l.sub.Receive(ctx, func(c context.Context, msg *pubsub.Message) {
		hold := time.Duration(l.minHoldDuration.Load())

		l.stateMu.Lock()
		l.lastMsgTime = time.Now()
		if !l.active {
			l.active = true
			l.broadcast(true)
			log.Printf("Topic %s ACTIVE", l.topicID)
		}

		// Cancel previous expiry trigger if it exists
		if l.holdTimer != nil {
			l.holdTimer.Stop()
		}

		// Spawn a new trigger for the expiry
		l.holdTimer = time.AfterFunc(hold, func() {
			l.stateMu.Lock()
			defer l.stateMu.Unlock()

			if l.active && time.Since(l.lastMsgTime) >= hold {
				l.active = false
				l.broadcast(false)
				log.Printf("Topic %s INACTIVE (idle for %s)", l.topicID, hold)
			}
		})
		l.stateMu.Unlock()

		// Hold the message for the check interval to prevent a tight Nack loop
		currentInterval := time.Duration(l.checkInterval.Load())
		select {
		case <-time.After(currentInterval):
		case <-c.Done():
		}

		// Purge the backlog right before Nacking. This clears the client buffer
		// and ensures the next poll is for fresh activity.
		l.purge()
		msg.Nack()
	})

	if err != nil {
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.NotFound {
			log.Printf("Topic or subscription not found for %s. Stopping listener.", l.topicID)
			return
		}
		log.Printf("Receive error for topic %s: %v", l.topicID, err)
	}
}

func (l *TopicListener) purge() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := l.sub.SeekToTime(ctx, time.Now()); err != nil {
		log.Printf("Error purging sub %s: %v", l.sub.ID(), err)
	}
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
