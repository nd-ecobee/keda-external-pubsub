package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	pb "github.com/kedacore/keda/v2/pkg/scalers/externalscaler"
)

func NewPubSubScaler() *PubSubScaler {
	projectID := os.Getenv("GCP_PROJECT_ID")
	if projectID == "" {
		projectID = os.Getenv("GOOGLE_CLOUD_PROJECT")
	}

	ctx := context.Background()
	podPSClient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("failed to create pubsub client: %v", err)
	}

	return &PubSubScaler{
		podPSClient:  podPSClient,
		podProjectID: projectID,
		listeners:    make(map[string]*TopicListener),
	}
}

func (s *PubSubScaler) getManager(meta map[string]string) (*SubscriptionManager, error) {
	topic := meta["topic"]
	sub := meta["subscription"]
	holdStr := meta["holdDuration"]

	if topic == "" || sub == "" {
		return nil, fmt.Errorf("topic and subscription are required in metadata")
	}

	hold, err := time.ParseDuration(holdStr)
	if err != nil {
		hold = 5 * time.Minute // Default hold duration 5m
	}

	key := fmt.Sprintf("%s", topic)

	if m, ok := s.managers.Load(key); ok {
		return m.(*SubscriptionManager), nil
	}

	m := &SubscriptionManager{
		scaler:       s,
		topicName:    topic,
		workerSub:    sub,
		holdDuration: hold,
		msgNotify:    make(chan struct{}, 1),
		stopCh:       make(chan struct{}),
	}

	// Try to store, if someone else beat us to it, return theirs
	actual, loaded := s.managers.LoadOrStore(key, m)
	if loaded {
		return actual.(*SubscriptionManager), nil
	}

	listener, err := s.getListener(topic)
	if err != nil {
		// Clean up if listener creation fails
		s.managers.Delete(key)
		return nil, err
	}
	listener.register(m)

	go m.run()
	return m, nil
}

func splitGCPResource(res string) []string {
	return strings.Split(res, "/")
}

func (s *PubSubScaler) IsActive(ctx context.Context, ref *pb.ScaledObjectRef) (*pb.IsActiveResponse, error) {
	m, err := s.getManager(ref.ScalerMetadata)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return &pb.IsActiveResponse{Result: m.active}, nil
}

func (s *PubSubScaler) StreamIsActive(ref *pb.ScaledObjectRef, stream pb.ExternalScaler_StreamIsActiveServer) error {
	m, err := s.getManager(ref.ScalerMetadata)
	if err != nil {
		return err
	}

	ch := make(chan bool, 1)
	m.streams.Store(ch, struct{}{})

	defer func() {
		m.streams.Delete(ch)
	}()

	m.mu.RLock()
	initialActive := m.active
	m.mu.RUnlock()
	if err := stream.Send(&pb.IsActiveResponse{Result: initialActive}); err != nil {
		return err
	}

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case active := <-ch:
			if err := stream.Send(&pb.IsActiveResponse{Result: active}); err != nil {
				return err
			}
		}
	}
}

func (s *PubSubScaler) GetMetricSpec(ctx context.Context, ref *pb.ScaledObjectRef) (*pb.GetMetricSpecResponse, error) {
	return &pb.GetMetricSpecResponse{
		MetricSpecs: []*pb.MetricSpec{
			{
				MetricName: "has_pending_message",
				TargetSize: 1,
			},
		},
	}, nil
}

func (s *PubSubScaler) GetMetrics(ctx context.Context, req *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	m, err := s.getManager(req.ScaledObjectRef.ScalerMetadata)
	if err != nil {
		return nil, err
	}

	// Fetch actual backlog from PQL
	val, err := s.getWorkerBacklogPQL(m)
	if err != nil {
		log.Printf("Error fetching backlog for %s: %v", m.workerSub, err)
		val = 0
	}

	return &pb.GetMetricsResponse{
		MetricValues: []*pb.MetricValue{
			{
				MetricName:  "has_pending_message",
				MetricValue: val,
			},
		},
	}, nil
}
