package main

import (
	"log"
	"time"
)

func NewSubscriptionManager(promService *PrometheusService, subProject string, subID string, hold time.Duration, check time.Duration, isTLActive func() bool) *SubscriptionManager {
	m := &SubscriptionManager{
		promService:      promService,
		workerSubProject: subProject,
		workerSubID:      subID,
		holdDuration:     hold,
		checkInterval:    check,
		msgNotify:        make(chan struct{}, 1),
		isTLActive:       isTLActive,
		stopCh:           make(chan struct{}),
	}

	// Synchronously populate state in New
	m.active.Store(isTLActive() || m.isActiveByMetrics())
	go m.run()

	return m
}

func (m *SubscriptionManager) isActiveByMetrics() bool {
	backlog, err := m.promService.GetWorkerBacklog(m.workerSubProject, m.workerSubID, m.holdDuration)
	if err != nil {
		log.Printf("Error checking backlog in isActiveByMetrics for sub %s: %v", m.workerSubID, err)
		return false
	}
	return backlog > 0
}

func (m *SubscriptionManager) IsActive() bool {
	return m.active.Load()
}

func (m *SubscriptionManager) run() {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-m.msgNotify:
			if m.active.CompareAndSwap(false, true) {
				m.broadcast(true)
				log.Printf("[Sub: %s] ACTIVE", m.workerSubID)
			}
		case <-ticker.C:
			m.checkDeactivation()
		}
	}
}

func (m *SubscriptionManager) checkDeactivation() {
	// Combine all conditions that prevent deactivation into one check.
	if !m.active.Load() || m.isTLActive() || m.isActiveByMetrics() {
		return
	}

	if m.active.CompareAndSwap(true, false) {
		m.broadcast(false)
		log.Printf("[Sub: %s] INACTIVE", m.workerSubID)
	}
}

func (m *SubscriptionManager) broadcast(active bool) {
	m.streams.Range(func(key, value interface{}) bool {
		ch := key.(chan bool)
		select {
		case ch <- active:
		default:
		}
		return true
	})
}
