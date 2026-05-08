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

func NewTopicListener(promService *PrometheusService, podPSClient *pubsub.Client, podProjectID string, topicID string) (*TopicListener, error) {
	topicParts := splitGCPResource(topicID)
	if len(topicParts) != 4 {
		return nil, fmt.Errorf("topic ID must be in the format 'projects/<project>/topics/<topic>', got: %s", topicID)
	}
	topicProject := topicParts[1]
	topicName := topicParts[3]

	pCtx, pCancel := context.WithCancel(context.Background())
	l := &TopicListener{
		promService:  promService,
		podProjectID: podProjectID,
		topicID:      topicID,
		topicProject: topicProject,
		topicName:    topicName,
		stopCh:       make(chan struct{}),
		purgeCtx:     pCtx,
		purgeCancel:  pCancel,
	}
	l.minHoldDuration.Store(int64(5 * time.Minute))

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
		sub = podPSClient.Subscription(subID)
	}
	l.sub = sub

	// Synchronously check state (metrics based) without locking in New
	l.active = l.isActiveByMetrics()

	go l.listen()
	go l.purgeLoop()
	
	return l, nil
}

func (l *TopicListener) isActiveByMetrics() bool {
	extDuration := time.Duration(l.minHoldDuration.Load())
	count, err := l.promService.GetTopicPublishRate(l.topicProject, l.topicName, extDuration)
	if err != nil {
		log.Printf("Error checking publish rate in isActiveByMetrics for %s: %v", l.topicID, err)
		return false
	}
	return count > 0
}

func (l *TopicListener) IsActive() bool {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
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

	l, err := NewTopicListener(s.promService, s.podPSClient, s.podProjectID, topicID)
	if err != nil {
		return nil, err
	}

	s.listeners[topicID] = l
	return l, nil
}

func (l *TopicListener) register(notifyCh chan struct{}, holdDuration time.Duration) {
	l.notifyChannels.Store(notifyCh, struct{}{})

	// Ensure we use the minimum requested duration across all observers,
	// but never go below an absolute floor of 30 seconds.
	l.minHoldDurationMu.Lock()
	defer l.minHoldDurationMu.Unlock()

	currentMin := time.Duration(l.minHoldDuration.Load())
	effectiveHold := max(30*time.Second, holdDuration)
	l.minHoldDuration.Store(int64(min(currentMin, effectiveHold)))
}

func (l *TopicListener) listen() {
	ctx := context.Background()

	l.sub.ReceiveSettings.MaxOutstandingMessages = 1
	l.sub.ReceiveSettings.NumGoroutines = 1
	l.sub.ReceiveSettings.Synchronous = true
	l.sub.ReceiveSettings.MaxExtension = time.Duration(l.minHoldDuration.Load()) + 1*time.Minute

	log.Printf("Starting listener for topic %s on sub %s", l.topicID, l.sub.ID())

	err := l.sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		l.stateMu.Lock()
		l.active = true
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
			if l.active {
				l.active = false
			}
			l.stateMu.Unlock()
		}
		msg.Nack()
	})

	if err != nil {
		log.Fatalf("CRITICAL: Receive error for topic %s: %v. Crashing pod for restart.", l.topicID, err)
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

			count, err := l.promService.GetTopicPublishRate(l.topicProject, l.topicName, minHoldDuration)
			if err != nil {
				log.Printf("Error checking publish rate for %s: %v", l.topicID, err)
				continue
			}

			// If publish count is 0 over the minHoldDuration, purge the ephemeral sub
			if count == 0 {
				log.Printf("Topic %s publish increase is 0 over %s. Purging ephemeral listener sub.", l.topicID, minHoldDuration)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := l.sub.SeekToTime(ctx, time.Now()); err != nil {
					log.Printf("Error purging sub %s: %v", l.sub.ID(), err)
				}
				cancel()

				// Release the held message and prepare a new context for the next trigger
				l.stateMu.Lock()
				if l.active {
					l.active = false
					l.purgeCancel()
					l.purgeCtx, l.purgeCancel = context.WithCancel(context.Background())
				}
				l.stateMu.Unlock()
			}
		}
	}
}
