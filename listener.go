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

	l := &TopicListener{
		scaler:    s,
		topicName: topicName,
		observers: make(map[*SubscriptionManager]struct{}),
		stopCh:    make(chan struct{}),
	}

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

func (l *TopicListener) register(m *SubscriptionManager) {
	l.observersMu.Lock()
	defer l.observersMu.Unlock()
	l.observers[m] = struct{}{}
}

func (l *TopicListener) listen() {
	ctx := context.Background()
	l.sub.ReceiveSettings.MaxOutstandingMessages = 1
	l.sub.ReceiveSettings.NumGoroutines = 1
	l.sub.ReceiveSettings.Synchronous = true

	log.Printf("Starting listener for topic %s on sub %s", l.topicName, l.sub.ID())
	
	err := l.sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		l.observersMu.RLock()
		for m := range l.observers {
			select {
			case m.msgNotify <- struct{}{}:
			default:
			}
		}
		l.observersMu.RUnlock()
		// DO NOT ACK. Rely on the purge loop to clear messages when the topic is idle by metrics.
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
			// Determine the maximum hold duration among all observers
			l.observersMu.RLock()
			holdDuration := 5 * time.Minute // default minimum
			for m := range l.observers {
				if m.holdDuration > holdDuration {
					holdDuration = m.holdDuration
				}
			}
			l.observersMu.RUnlock()

			count, err := l.scaler.getTopicPublishRatePQL(l.topicName, holdDuration)
			if err != nil {
				log.Printf("Error checking publish rate for %s: %v", l.topicName, err)
				continue
			}

			// If publish count is 0 over the holdDuration, purge the ephemeral sub
			// This clears out any old messages that might be stuck if the pod was down
			// while the topic was inactive.
			if count == 0 {
				log.Printf("Topic %s publish increase is 0 over %s. Purging ephemeral listener sub.", l.topicName, holdDuration)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := l.sub.SeekToTime(ctx, time.Now()); err != nil {
					log.Printf("Error purging sub %s: %v", l.sub.ID(), err)
				}
				cancel()
			}
		}
	}
}
