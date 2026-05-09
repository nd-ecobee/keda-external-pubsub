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

type TopicListener struct {
	podPSClient  *pubsub.Client
	topicID      string // full resource name
	topicProject string
	topicName    string // short name
	sub          *pubsub.Subscription
	
	// Channels to notify when active state changes. Key: chan bool, Value: struct{}
	notifyChannels sync.Map

	minHoldDuration   atomic.Int64
	minHoldDurationMu sync.Mutex

	checkInterval   atomic.Int64
	checkIntervalMu sync.Mutex

	stateMu     sync.RWMutex
	active      bool
	lastMsgTime time.Time
	holdTimer   *time.Timer

	stopCh chan struct{}
}
