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

type TopicListener struct {
	client              *pubsub.Client
	topic               *pubsub.Topic
	subID               string
	minDebounceDuration *atomic.Int64
	checkInterval       *atomic.Int64
	configMu            *sync.Mutex

	notifyChannels sync.Map
	streamCount    atomic.Int32
	reconcileCh    chan bool
	isActive       atomic.Bool

	runCtx context.Context
}

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

	ctx := context.Background()

	l := &TopicListener{
		client:              podPSClient,
		topic:               topic,
		subID:               subID,
		minDebounceDuration: minDebounce,
		checkInterval:       checkInt,
		configMu:            &sync.Mutex{},
		reconcileCh:         make(chan bool, 1),
		runCtx:              ctx,
	}

	go l.controlLoop()

	return l, nil
}

func (l *TopicListener) updateConfig(debounceDuration, checkInterval time.Duration) bool {
	l.configMu.Lock()
	defer l.configMu.Unlock()

	currentMin := time.Duration(l.minDebounceDuration.Load())
	effectiveDebounce := max(30*time.Second, debounceDuration)
	l.minDebounceDuration.Store(int64(min(currentMin, effectiveDebounce)))

	currentCheck := time.Duration(l.checkInterval.Load())
	newCheck := int64(min(currentCheck, checkInterval))
	oldCheck := l.checkInterval.Swap(newCheck)

	return oldCheck != 0 && oldCheck != newCheck
}

func (l *TopicListener) ensureSubscription() error {
	_, err := l.client.CreateSubscription(l.runCtx, l.subID, pubsub.SubscriptionConfig{
		Topic:            l.topic,
		ExpirationPolicy: 24 * time.Hour,
	})
	if st, _ := status.FromError(err); st.Code() == codes.AlreadyExists {
		log.Printf("Verified subscription %s exists", l.subID)
		return nil
	} else if err != nil {
		log.Printf("Warning: failed to create subscription %s, retrying: %v", l.subID, err)
	} else {
		log.Printf("Successfully created subscription %s", l.subID)
	}
	return err
}

func (l *TopicListener) setActive(active bool) {
	if active == l.isActive.Load() {
		return
	}
	l.isActive.Store(active)
	log.Printf("Topic %s: %v", l.topic.String(), active)
	l.notifyChannels.Range(func(key, value any) bool {
		ch := key.(chan bool)
		TrySend(ch, active)
		return true
	})
}

func (l *TopicListener) controlLoop() {
	messageTick := make(chan any)
	holdTimer := time.NewTimer(time.Hour)
	holdTimer.Stop()

	for {
		if l.runCtx.Err() != nil {
			log.Printf("TopicListener control loop exiting for topic %s: %v", l.topic.String(), l.runCtx.Err())
			return
		}

		operation := func() (any, error) {
			return nil, l.ensureSubscription()
		}

		_, err := backoff.Retry(l.runCtx, operation, backoff.WithBackOff(backoff.NewExponentialBackOff()))
		if err != nil {
			log.Printf("TopicListener control loop exiting for topic %s due to backoff error: %v", l.topic.String(), err)
			return // runCtx canceled
		}

		opCtx, opCancel := context.WithCancel(context.Background())
		opCancel() // start canceled
		opDoneCh := make(chan any)
		close(opDoneCh)

		stopOperation := func() {
			opCancel()
			<-opDoneCh
		}
		startOperation := func() {
			stopOperation()
			opCtx, opCancel = context.WithCancel(l.runCtx)
			opDoneCh = make(chan any)

			sub := &topicSubscription{
				sub:           l.client.Subscription(l.subID),
				checkInterval: time.Duration(l.checkInterval.Load()),
				messageTick:   messageTick,
				runCtx:        opCtx,
			}

			go func() {
				defer close(opDoneCh)
				_, _ = backoff.Retry(opCtx, sub.Run, backoff.WithBackOff(backoff.NewExponentialBackOff()))
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
				holdTimer.Reset(time.Duration(l.minDebounceDuration.Load()))

			case <-holdTimer.C:
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

func (l *TopicListener) Register(notifyCh chan bool, debounceDuration time.Duration, checkInterval time.Duration) {
	l.notifyChannels.Store(notifyCh, nil)

	needsRecreate := l.updateConfig(debounceDuration, checkInterval)
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
