package main

import (
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/kedacore/keda/v2/pkg/scalers/externalscaler"
)

const (
	MetricHasTriggerMessage       = "has_trigger_message"
	MetricTopicMetricActive       = "topic_metric_active"
	MetricSubscriptionMetricActive = "subscription_metric_active"
	MetricHasPendingMessage       = "has_pending_message"
)

type PubSubScaler struct {
	externalscaler.UnimplementedExternalScalerServer
	
	promService *PrometheusService
	
	// Client for creating ephemeral subscriptions in the pod's project
	podPSClient *pubsub.Client

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
	podPSClient  *pubsub.Client
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
	active      atomic.Bool
	hasMsg      atomic.Bool

	// Cache for last known metric state
	lastMetricActive atomic.Bool

	stopCh chan struct{}
}

type SubscriptionManager struct {
	promService      *PrometheusService
	workerSubProject string
	workerSubID      string
	holdDuration     time.Duration
	checkInterval    time.Duration

	active atomic.Bool
	
	// Cache for last known metric state
	lastMetricActive atomic.Bool

	// Channel to receive "message arrived" pings from the listener
	msgNotify chan struct{}

	isTLActive func() bool

	streams sync.Map

	stopCh chan struct{}
}
