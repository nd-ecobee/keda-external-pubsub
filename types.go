package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/kedacore/keda/v2/pkg/scalers/externalscaler"
)

type PubSubScaler struct {
	externalscaler.UnimplementedExternalScalerServer
	
	promService *PrometheusService
	
	// Client for creating ephemeral subscriptions in the pod's project
	podPSClient *pubsub.Client
	podProjectID string

	// Listeners handle the actual StreamingPull from a topic (shared per topic)
	listeners   map[string]*TopicListener
	listenersMu sync.RWMutex

	// Managers handle the SO-specific (topic+sub) scaling logic
	managers sync.Map
}

type PrometheusService struct {
	clients sync.Map
}

type TopicListener struct {
	promService  *PrometheusService
	podProjectID string
	topicID      string // full resource name
	topicProject string
	topicName    string // short name
	sub          *pubsub.Subscription
	
	// Channels to notify when a message arrives. Key: chan struct{}, Value: struct{}
	notifyChannels sync.Map

	// The minimum hold duration among all registered observers
	minHoldDuration   atomic.Int64
	minHoldDurationMu sync.Mutex

	// The minimum check interval among all registered observers
	checkInterval   atomic.Int64
	checkIntervalMu sync.Mutex

	// State for holding a single message
	stateMu     sync.Mutex
	active      bool
	purgeCtx    context.Context
	purgeCancel context.CancelFunc

	stopCh chan struct{}
}

type SubscriptionManager struct {
	promService      *PrometheusService
	workerSubProject string
	workerSubID      string
	holdDuration     time.Duration
	checkInterval    time.Duration

	active       bool
	mu           sync.RWMutex

	// Channel to receive "message arrived" pings from the listener
	msgNotify chan struct{}

	isTLActive func() bool

	streams sync.Map

	stopCh chan struct{}
}
