package main

import (
	"context"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
)

type topicSubscription struct {
	sub           *pubsub.Subscription
	checkInterval time.Duration
	messageTick   chan<- any

	runCtx context.Context
}

func (s *topicSubscription) Run() (any, error) {
	log.Printf("Starting stream pull for subscription %s", s.sub.ID())
	defer log.Printf("Stream pull for subscription %s stopped", s.sub.ID())

	s.sub.ReceiveSettings.MaxOutstandingMessages = 1
	s.sub.ReceiveSettings.NumGoroutines = 1
	s.sub.ReceiveSettings.Synchronous = true
	s.sub.ReceiveSettings.MaxExtension = s.checkInterval + 1*time.Minute

	return nil, s.sub.Receive(s.runCtx, s.processMessage)
}

func (s *topicSubscription) processMessage(c context.Context, msg *pubsub.Message) {
	log.Printf("Received message %s on subscription %s", msg.ID, s.sub.ID())

	select {
	case s.messageTick <- nil:
	case <-s.runCtx.Done():
	}

	// Hold the message for the check interval to prevent a tight Nack loop
	select {
	case <-time.After(s.checkInterval):
	case <-c.Done():
	}

	// Purge the backlog right before Nacking. This clears the client buffer
	// and ensures the next poll is for fresh activity.
	s.purge()
	msg.Nack()
	log.Printf("Nacked message %s on subscription %s", msg.ID, s.sub.ID())
}

func (s *topicSubscription) purge() {
	log.Printf("Purging backlog for subscription %s", s.sub.ID())
	// The shared runCtx handles scaler teardown, plus a timeout so SeekToTime doesn't block indefinitely
	ctx, cancel := context.WithTimeout(s.runCtx, 5*time.Second)
	defer cancel()
	if err := s.sub.SeekToTime(ctx, time.Now()); err != nil {
		log.Printf("Error purging sub %s: %v", s.sub.ID(), err)
	}
}
