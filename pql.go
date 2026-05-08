package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"golang.org/x/oauth2/google"
)

type tokenAuthTransport struct {
	base http.RoundTripper
}

func (t *tokenAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	ts, err := google.DefaultTokenSource(ctx, "https://www.googleapis.com/auth/monitoring.read")
	if err != nil {
		return nil, err
	}
	token, err := ts.Token()
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)
	return t.base.RoundTrip(req)
}

func NewPQLClient(projectID string) v1.API {
	address := fmt.Sprintf("https://monitoring.googleapis.com/v1/projects/%s/location/global/prometheus", projectID)
	
	conf := api.Config{
		Address: address,
		RoundTripper: &tokenAuthTransport{
			base: http.DefaultTransport,
		},
	}

	client, err := api.NewClient(conf)
	if err != nil {
		log.Fatalf("error creating prometheus client: %v", err)
	}

	return v1.NewAPI(client)
}

func (s *PubSubScaler) getPQLClient(projectID string) v1.API {
	if c, ok := s.pqlClients.Load(projectID); ok {
		return c.(v1.API)
	}
	c := NewPQLClient(projectID)
	actual, _ := s.pqlClients.LoadOrStore(projectID, c)
	return actual.(v1.API)
}

func (s *PubSubScaler) getWorkerBacklogPQL(m *SubscriptionManager) (int64, error) {
	subID := m.workerSub
	subParts := splitGCPResource(m.workerSub)
	projectID := s.podProjectID
	if len(subParts) >= 4 {
		projectID = subParts[1]
		subID = subParts[3]
	}

	// Use max_over_time over the holdDuration to ensure it has been 0 consistently.
	// This implements the requirement: "backlog is 0 for at least hold duration"
	query := fmt.Sprintf(
		"max_over_time({__name__=\"pubsub.googleapis.com/subscription/num_undelivered_messages\", monitored_resource=\"pubsub_subscription\", subscription_id=\"%s\"}[%s])",
		subID,
		m.holdDuration.String(),
	)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	val, _, err := s.getPQLClient(projectID).Query(ctx, query, time.Now())
	if err != nil {
		return -1, err
	}

	vector, ok := val.(model.Vector)
	if !ok {
		return -1, fmt.Errorf("unexpected query result type: %T", val)
	}

	if len(vector) == 0 {
		return 0, nil // No data = no backlog
	}

	return int64(vector[0].Value), nil
}

func (s *PubSubScaler) getTopicPublishRatePQL(topicName string, holdDuration time.Duration) (float64, error) {
	topicParts := splitGCPResource(topicName)
	var topicID string
	projectID := s.podProjectID
	if len(topicParts) >= 4 {
		projectID = topicParts[1]
		topicID = topicParts[3]
	} else {
		topicID = topicName
	}

	// PromQL to get the total increase of published messages over the hold duration
	query := fmt.Sprintf(
		"sum(increase({__name__=\"pubsub.googleapis.com/topic/send_request_count\", monitored_resource=\"pubsub_topic\", topic_id=\"%s\"}[%s]))",
		topicID,
		holdDuration.String(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	val, _, err := s.getPQLClient(projectID).Query(ctx, query, time.Now())
	if err != nil {
		return -1, err
	}

	vector, ok := val.(model.Vector)
	if !ok {
		return -1, fmt.Errorf("unexpected query result type: %T", val)
	}

	if len(vector) == 0 {
		return 0, nil // No data = rate is 0
	}

	return float64(vector[0].Value), nil
}
