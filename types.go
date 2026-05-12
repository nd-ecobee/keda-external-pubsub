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

type ListenerConfig struct {
	Client          *pubsub.Client
	Topic           *pubsub.Topic
	SubID           string
	MinDebounceDuration *atomic.Int64
	CheckInterval   *atomic.Int64
	ConfigMu        *sync.Mutex
}

func (c *ListenerConfig) UpdateConfig(debounceDuration, checkInterval time.Duration) bool {
	c.ConfigMu.Lock()
	defer c.ConfigMu.Unlock()

	currentMin := time.Duration(c.MinDebounceDuration.Load())
	effectiveDebounce := max(30*time.Second, debounceDuration)
	c.MinDebounceDuration.Store(int64(min(currentMin, effectiveDebounce)))

	currentCheck := time.Duration(c.CheckInterval.Load())
	newCheck := int64(min(currentCheck, checkInterval))
	oldCheck := c.CheckInterval.Swap(newCheck)

	return oldCheck != 0 && oldCheck != newCheck
}

type TopicListener struct {
	updateConfig   func(time.Duration, time.Duration) bool
	notifyChannels sync.Map
	streamCount    atomic.Int32
	reconcileCh    chan bool
	isActive       atomic.Bool

	runCtx context.Context
}

type receiveOperation struct {
	sub                 *pubsub.Subscription
	minDebounceDuration *atomic.Int64
	checkInterval       time.Duration
	messageTick         chan<- any

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
