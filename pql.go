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

func NewPrometheusService() *PrometheusService {
	return &PrometheusService{}
}

func (s *PrometheusService) getClient(projectID string) v1.API {
	if c, ok := s.clients.Load(projectID); ok {
		return c.(v1.API)
	}
	c := NewPQLClient(projectID)
	actual, _ := s.clients.LoadOrStore(projectID, c)
	return actual.(v1.API)
}

func (s *PrometheusService) GetWorkerBacklog(projectID, subID string, holdDuration time.Duration) (int64, error) {
	query := fmt.Sprintf(
		"max_over_time({__name__=\"pubsub.googleapis.com/subscription/num_undelivered_messages\", monitored_resource=\"pubsub_subscription\", subscription_id=\"%s\"}[%s])",
		subID,
		holdDuration.String(),
	)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	val, _, err := s.getClient(projectID).Query(ctx, query, time.Now())
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

func (s *PrometheusService) GetTopicPublishRate(projectID, topicID string, holdDuration time.Duration) (float64, error) {
	query := fmt.Sprintf(
		"sum(increase({__name__=\"pubsub.googleapis.com/topic/send_request_count\", monitored_resource=\"pubsub_topic\", topic_id=\"%s\"}[%s]))",
		topicID,
		holdDuration.String(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	val, _, err := s.getClient(projectID).Query(ctx, query, time.Now())
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
