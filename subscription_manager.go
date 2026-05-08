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
	m.active = isTLActive() || m.isActiveByMetrics()
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
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.active
}

func (m *SubscriptionManager) run() {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-m.msgNotify:
			m.mu.Lock()
			if !m.active {
				m.active = true
				m.broadcast(true)
				log.Printf("[Sub: %s] ACTIVE", m.workerSubID)
			}
			m.mu.Unlock()
		case <-ticker.C:
			m.checkDeactivation()
		}
	}
}

func (m *SubscriptionManager) checkDeactivation() {
	m.mu.Lock()
	defer m.mu.RUnlock()

	// Combine all conditions that prevent deactivation into one check.
	// We only deactivate if we are currently active AND the topic is idle AND the metrics have cleared.
	if !m.active || m.isTLActive() || m.isActiveByMetrics() {
		return
	}

	m.active = false
	m.broadcast(false)
	log.Printf("[Sub: %s] INACTIVE", m.workerSubID)
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
