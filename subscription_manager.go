package main

import (
	"log"
	"time"
)

func (m *SubscriptionManager) run() {
	// Initial ticker for deactivation checks
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var timerCh <-chan time.Time

	for {
		select {
		case <-m.stopCh:
			return
		case <-m.msgNotify:
			m.mu.Lock()
			if !m.active {
				m.active = true
				m.firstMsgTime = time.Now()
				m.broadcast(true)
				log.Printf("[Topic: %s | Sub: %s] ACTIVE. Hold duration: %s", m.topicName, m.workerSub, m.holdDuration)
				
				// Set a timer to wake up exactly when the hold duration expires
				timerCh = time.After(m.holdDuration)
			}
			m.mu.Unlock()
		case <-timerCh:
			// Hold duration reached! Check immediately.
			m.checkDeactivation()
			timerCh = nil // Reset so we don't trigger again until next activation
		case <-ticker.C:
			// Periodic check in case backlog was not zero at the exact moment of timer expiry
			m.checkDeactivation()
		}
	}
}

func (m *SubscriptionManager) checkDeactivation() {
	m.mu.Lock()
	if !m.active {
		m.mu.Unlock()
		return
	}

	if time.Since(m.firstMsgTime) < m.holdDuration {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()

	backlog, err := m.scaler.getWorkerBacklogPQL(m)
	if err != nil {
		log.Printf("Error checking backlog for %s: %v", m.workerSub, err)
		return
	}

	if backlog == 0 {
		m.mu.Lock()
		m.active = false
		m.broadcast(false)
		log.Printf("[Topic: %s | Sub: %s] INACTIVE", m.topicName, m.workerSub)
		m.mu.Unlock()
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
