package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/kedacore/keda/v2/pkg/scalers/externalscaler"
)

const (
	MetricHasPendingMessage = "has_pending_message"
)

type PubSubScaler struct {
	externalscaler.UnimplementedExternalScalerServer

	podPSClient *pubsub.Client

	listeners   map[string]*TopicListener
	listenersMu sync.RWMutex
}

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

type receiveOperation struct {
	sub           *pubsub.Subscription
	checkInterval time.Duration
	messageTick   chan<- any

	runCtx context.Context
}

func TrySend[T any](ch chan<- T, value T) bool {
	select {
	case ch <- value:
		return true
	default:
		return false
	}
}
