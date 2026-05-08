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

func NewTopicListener(promService *PrometheusService, podPSClient *pubsub.Client, podProjectID string, topicID string, checkInterval time.Duration) (*TopicListener, error) {
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
	l.checkInterval.Store(int64(checkInterval))

	h := fnv.New32a()
	podName, _ := os.Hostname()
	h.Write([]byte(topicID + podName))
	subID := fmt.Sprintf("keda-%s-%x", topicName, h.Sum32())

	ctx := context.Background()
	topic := podPSClient.TopicInProject(topicID, topicProject)
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

func (s *PubSubScaler) getListener(topicID string, checkInterval time.Duration) (*TopicListener, error) {
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

	l, err := NewTopicListener(s.promService, s.podPSClient, s.podProjectID, topicID, checkInterval)
	if err != nil {
		return nil, err
	}

	s.listeners[topicID] = l
	return l, nil
}

func (l *TopicListener) register(notifyCh chan struct{}, holdDuration time.Duration, checkInterval time.Duration) {
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

	// If currently active, immediately notify the new observer
	if l.IsActive() {
		select {
		case notifyCh <- struct{}{}:
		default:
		}
	}
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
			// The purge loop has already handled setting l.active = false
		case <-time.After(holdDuration):
			l.stateMu.Lock()
			// Only clear active if the topic is actually empty by metrics.
			// If not empty, we keep l.active = true, Nack, and let the loop pull again.
			if l.active && !l.isActiveByMetrics() {
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
	for {
		checkInterval := time.Duration(l.checkInterval.Load())
		select {
		case <-l.stopCh:
			return
		case <-time.After(checkInterval):
			// If topic is inactive by metrics, purge the ephemeral sub
			if !l.isActiveByMetrics() {
				minHoldDuration := time.Duration(l.minHoldDuration.Load())

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
