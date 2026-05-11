package main

import (
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
	Client       *pubsub.Client
	TopicID      string // full resource name
	TopicProject string
	TopicName    string // short name
	Topic        *pubsub.Topic
	SubID        string

	MinHoldDuration   *atomic.Int64
	MinHoldDurationMu *sync.Mutex

	CheckInterval   *atomic.Int64
	CheckIntervalMu *sync.Mutex
}

func (c *ListenerConfig) UpdateConfig(holdDuration, checkInterval time.Duration) bool {
	c.MinHoldDurationMu.Lock()
	currentMin := time.Duration(c.MinHoldDuration.Load())
	if currentMin == 0 {
		currentMin = 5 * time.Minute
	}
	effectiveHold := max(30*time.Second, holdDuration)
	c.MinHoldDuration.Store(int64(min(currentMin, effectiveHold)))
	c.MinHoldDurationMu.Unlock()

	c.CheckIntervalMu.Lock()
	currentCheck := time.Duration(c.CheckInterval.Load())
	if currentCheck == 0 {
		currentCheck = 1 * time.Minute
	}
	newCheck := int64(min(currentCheck, checkInterval))
	oldCheck := c.CheckInterval.Swap(newCheck)
	c.CheckIntervalMu.Unlock()

	return oldCheck != 0 && oldCheck != newCheck
}

type TopicListener struct {
	updateConfig func(time.Duration, time.Duration) bool
	
	// Channels to notify when active state changes. Key: chan bool, Value: struct{}
	notifyChannels sync.Map

	activeOp atomic.Pointer[receiveOperation]

	stateCh    chan bool
	recreateCh chan struct{}
	stopCh     chan struct{}
}

type receiveOperation struct {
	config  ListenerConfig
	sub     *pubsub.Subscription
	stateCh chan<- bool

	stateMu   sync.RWMutex
	active    bool
	lease     uint64
	holdTimer *time.Timer
}

