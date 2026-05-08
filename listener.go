package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
)

func (s *PubSubScaler) getListener(topicName string) (*TopicListener, error) {
	s.listenersMu.RLock()
	if l, ok := s.listeners[topicName]; ok {
		s.listenersMu.RUnlock()
		return l, nil
	}
	s.listenersMu.RUnlock()

	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()

	// Double-check after acquiring write lock
	if l, ok := s.listeners[topicName]; ok {
		return l, nil
	}

	// Extract project ID from topic: projects/{project}/topics/{topic}
	var topicProject string
	fmt.Sscanf(topicName, "projects/%s/topics/", &topicProject)

	pCtx, pCancel := context.WithCancel(context.Background())
	l := &TopicListener{
		scaler:      s,
		topicName:   topicName,
		stopCh:      make(chan struct{}),
		purgeCtx:    pCtx,
		purgeCancel: pCancel,
	}
	l.minHoldDuration.Store(int64(5 * time.Minute))

	h := fnv.New32a()
	h.Write([]byte(topicName))
	podName, _ := os.Hostname()
	subID := fmt.Sprintf("keda-push-%x-%s", h.Sum32(), podName)

	ctx := context.Background()
	topic := s.podPSClient.TopicInProject(topicName, topicProject)
	sub, err := s.podPSClient.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:            topic,
		ExpirationPolicy: 24 * time.Hour,
	})
	if err != nil {
		sub = s.podPSClient.Subscription(subID)
	}
	l.sub = sub

	go l.listen()
	go l.purgeLoop()
	
	s.listeners[topicName] = l
	return l, nil
}

func (l *TopicListener) register(notifyCh chan struct{}, holdDuration time.Duration) {
	l.notifyChannels.Store(notifyCh, struct{}{})

	l.minHoldDurationMu.Lock()
	currentMin := time.Duration(l.minHoldDuration.Load())
	if currentMin == 0 || holdDuration < currentMin {
		l.minHoldDuration.Store(int64(holdDuration))
	}
	l.minHoldDurationMu.Unlock()

	// If currently holding a message (topic is active), immediately notify the new observer
	l.stateMu.Lock()
	if l.isHolding {
		select {
		case notifyCh <- struct{}{}:
		default:
		}
	}
	l.stateMu.Unlock()
}

func (l *TopicListener) listen() {
	ctx := context.Background()

	extDuration := time.Duration(l.minHoldDuration.Load())

	l.sub.ReceiveSettings.MaxOutstandingMessages = 1
	l.sub.ReceiveSettings.NumGoroutines = 1
	l.sub.ReceiveSettings.Synchronous = true
	l.sub.ReceiveSettings.MaxExtension = extDuration + 1*time.Minute

	log.Printf("Starting listener for topic %s on sub %s", l.topicName, l.sub.ID())

	err := l.sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		l.stateMu.Lock()
		l.isHolding = true
		pCtx := l.purgeCtx
		l.stateMu.Unlock()

		l.notifyChannels.Range(func(key, value interface{}) bool {
			ch := key.(chan struct{})
			select {
			case ch <- struct{}{}:
			default:
			}
			return true
		})

		holdDuration := time.Duration(l.minHoldDuration.Load())

		// Keep exactly one message in the listener holding the flow-control token.
		// We set MaxExtension to a bit more than hold duration.
		// We nack the message at hold time or when the topic clears (pCtx canceled).
		select {
		case <-pCtx.Done():
		case <-time.After(holdDuration):
			l.stateMu.Lock()
			if l.isHolding {
				l.isHolding = false
			}
			l.stateMu.Unlock()
		}
		msg.Nack()
	})

	if err != nil {
		log.Fatalf("CRITICAL: Receive error for topic %s: %v. Crashing pod for restart.", l.topicName, err)
	}
}

func (l *TopicListener) purgeLoop() {
	// Check every 1 minute if the topic has any publish activity
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-l.stopCh:
			return
		case <-ticker.C:
			minHoldDuration := time.Duration(l.minHoldDuration.Load())

			count, err := l.scaler.getTopicPublishRatePQL(l.topicName, minHoldDuration)
			if err != nil {
				log.Printf("Error checking publish rate for %s: %v", l.topicName, err)
				continue
			}

			// If publish count is 0 over the minHoldDuration, purge the ephemeral sub
			// This clears out any old messages that might be stuck if the pod was down
			// while the topic was inactive.
			if count == 0 {
				log.Printf("Topic %s publish increase is 0 over %s. Purging ephemeral listener sub.", l.topicName, minHoldDuration)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := l.sub.SeekToTime(ctx, time.Now()); err != nil {
					log.Printf("Error purging sub %s: %v", l.sub.ID(), err)
				}
				cancel()

				// Release the held message and prepare a new context for the next trigger
				l.stateMu.Lock()
				if l.isHolding {
					l.isHolding = false
					l.purgeCancel()
					l.purgeCtx, l.purgeCancel = context.WithCancel(context.Background())
				}
				l.stateMu.Unlock()
			}
		}
	}
}
