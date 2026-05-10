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

func (c *ListenerConfig) UpdateHoldDuration(holdDuration time.Duration) {
	c.MinHoldDurationMu.Lock()
	defer c.MinHoldDurationMu.Unlock()
	currentMin := time.Duration(c.MinHoldDuration.Load())
	effectiveHold := max(30*time.Second, holdDuration)
	c.MinHoldDuration.Store(int64(min(currentMin, effectiveHold)))
}

func (c *ListenerConfig) UpdateCheckInterval(checkInterval time.Duration) {
	c.CheckIntervalMu.Lock()
	defer c.CheckIntervalMu.Unlock()
	currentCheck := time.Duration(c.CheckInterval.Load())
	c.CheckInterval.Store(int64(min(currentCheck, checkInterval)))
}

type TopicListener struct {
	config ListenerConfig
	
	// Channels to notify when active state changes. Key: chan bool, Value: struct{}
	notifyChannels sync.Map

	activeOp atomic.Pointer[receiveOperation]

	stopCh chan struct{}
}

type receiveOperation struct {
	config          ListenerConfig
	sub             *pubsub.Subscription
	minHoldDuration *atomic.Int64
	checkInterval   *atomic.Int64
	broadcast       func(bool)

	stateMu     sync.RWMutex
	active      bool
	lastMsgTime time.Time

	mu        sync.Mutex
	holdTimer *time.Timer
}

