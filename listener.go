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

	l := &TopicListener{
		promService:  promService,
		podProjectID: podProjectID,
		topicID:      topicID,
		topicProject: topicProject,
		topicName:    topicName,
		stopCh:       make(chan struct{}),
	}
	l.minHoldDuration.Store(int64(5 * time.Minute))
	l.checkInterval.Store(int64(checkInterval))

	h := fnv.New32a()
	podName, _ := os.Hostname()
	h.Write([]byte(topicID + podName))
	subID := fmt.Sprintf("keda-%s-%x", topicName, h.Sum32())

	ctx := context.Background()
	sub, err := podPSClient.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:            podPSClient.TopicInProject(topicID, topicProject),
		ExpirationPolicy: 24 * time.Hour,
	})
	if err != nil {
		sub = podPSClient.Subscription(subID)
	}
	l.sub = sub

	// Synchronously check state (metrics based)
	l.active.Store(l.isActiveByMetrics())

	go l.listen()

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
	return l.active.Load()
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

	// Background ticker for periodic deactivation checks
	go func() {
		for {
			interval := time.Duration(l.checkInterval.Load())
			select {
			case <-l.stopCh:
				return
			case <-time.After(interval):
				// Optimization: skip metric check if we are currently holding a message
				if l.hasMsg.Load() {
					continue
				}

				// Poll the metrics for update between purge and next message
				l.updateActiveState()
			}
		}
	}()

	err := l.sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		l.active.Store(true)
		l.hasMsg.Store(true)

		l.notifyChannels.Range(func(key, value interface{}) bool {
			ch := key.(chan struct{})
			select {
			case ch <- struct{}{}:
			default:
			}
			return true
		})

		hold := time.Duration(l.minHoldDuration.Load())

		// The message hold: block this callback for the required duration.
		time.Sleep(hold)

		l.hasMsg.Store(false)

		// 1. ALWAYS purge after hold
		l.purge()

		// 2. Update active state based on topic metrics (keep active if topic is busy)
		l.updateActiveState()

		msg.Nack()
	})

	if err != nil {
		log.Fatalf("CRITICAL: Receive error for topic %s: %v. Crashing pod for restart.", l.topicID, err)
	}
}

func (l *TopicListener) updateActiveState() {
	active := l.isActiveByMetrics()
	old := l.active.Swap(active)
	if old != active {
		log.Printf("Topic %s active state changed to: %v", l.topicID, active)
	}
}

func (l *TopicListener) purge() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := l.sub.SeekToTime(ctx, time.Now()); err != nil {
		log.Printf("Error purging sub %s: %v", l.sub.ID(), err)
	}
}
