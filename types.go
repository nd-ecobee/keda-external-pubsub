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
	
	pqlClients sync.Map
	
	// Client for creating ephemeral subscriptions in the pod's project
	podPSClient *pubsub.Client
	podProjectID string

	// Listeners handle the actual StreamingPull from a topic (shared per topic)
	listeners   map[string]*TopicListener
	listenersMu sync.RWMutex

	// Managers handle the SO-specific (topic+sub) scaling logic
	managers sync.Map
}

type TopicListener struct {
	scaler    *PubSubScaler
	topicName string
	sub       *pubsub.Subscription
	
	// Channels to notify when a message arrives. Key: chan struct{}, Value: struct{}
	notifyChannels sync.Map

	// The minimum hold duration among all registered observers
	minHoldDuration   atomic.Int64
	minHoldDurationMu sync.Mutex

	// State for holding a single message
	stateMu     sync.Mutex
	isHolding   bool
	purgeCtx    context.Context
	purgeCancel context.CancelFunc

	stopCh chan struct{}
}

type SubscriptionManager struct {
	scaler       *PubSubScaler
	topicName    string
	workerSub    string
	holdDuration time.Duration

	active       bool
	firstMsgTime time.Time
	mu           sync.RWMutex

	// Channel to receive "message arrived" pings from the listener
	msgNotify chan struct{}

	streams sync.Map

	stopCh chan struct{}
}
