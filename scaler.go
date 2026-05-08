package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	pb "github.com/kedacore/keda/v2/pkg/scalers/externalscaler"
)

func NewPubSubScaler() *PubSubScaler {
	ctx := context.Background()
	podPSClient, err := pubsub.NewClient(ctx, pubsub.DetectProjectID)
	if err != nil {
		log.Fatalf("failed to create pubsub client: %v", err)
	}

	return &PubSubScaler{
		promService: NewPrometheusService(),
		podPSClient: podPSClient,
		listeners:   make(map[string]*TopicListener),
	}
}

func (s *PubSubScaler) getManager(meta map[string]string) (*SubscriptionManager, error) {
	topicID := meta["topic"]
	sub := meta["subscription"]
	holdStr := meta["holdDuration"]
	checkStr := meta["checkInterval"]

	if topicID == "" || sub == "" {
		return nil, fmt.Errorf("topic and subscription are required in metadata")
	}

	hold, err := time.ParseDuration(holdStr)
	if err != nil {
		hold = 5 * time.Minute // Default hold duration 5m
	}

	check, err := time.ParseDuration(checkStr)
	if err != nil {
		check = 1 * time.Minute // Default check interval 1m
	}

	key := fmt.Sprintf("%s|%s", topicID, sub)

	if m, ok := s.managers.Load(key); ok {
		return m.(*SubscriptionManager), nil
	}

	// 1. s.getListener
	listener, err := s.getListener(topicID)
	if err != nil {
		return nil, err
	}

	// 2. NewSubscriptionManager
	subParts := splitGCPResource(sub)
	if len(subParts) != 4 {
		return nil, fmt.Errorf("subscription must be in the format 'projects/<project>/subscriptions/<sub>', got: %s", sub)
	}
	subProject := subParts[1]
	subID := subParts[3]

	m := NewSubscriptionManager(s.promService, subProject, subID, hold, check, listener.IsActive)

	// Try to store
	actual, loaded := s.managers.LoadOrStore(key, m)
	if loaded {
		return actual.(*SubscriptionManager), nil
	}

	// 3. listener.register()
	listener.register(m.msgNotify, m.holdDuration, m.checkInterval)

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
	return &pb.IsActiveResponse{Result: m.IsActive()}, nil
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

	initialActive := m.IsActive()
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
				MetricName:      MetricHasTriggerMessage,
				TargetSizeFloat: 1,
			},
			{
				MetricName:      MetricTopicMetricActive,
				TargetSizeFloat: 1,
			},
			{
				MetricName:      MetricSubscriptionMetricActive,
				TargetSizeFloat: 1,
			},
			{
				MetricName:      MetricHasPendingMessage,
				TargetSizeFloat: 1,
			},
		},
	}, nil
}

func (s *PubSubScaler) GetMetrics(ctx context.Context, req *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	m, err := s.getManager(req.ScaledObjectRef.ScalerMetadata)
	if err != nil {
		return nil, err
	}

	topicID := req.ScaledObjectRef.ScalerMetadata["topic"]
	listener, err := s.getListener(topicID)
	if err != nil {
		return nil, err
	}

	asFloat := func(b bool) float64 {
		if b {
			return 1
		}
		return 0
	}

	return &pb.GetMetricsResponse{
		MetricValues: []*pb.MetricValue{
			{
				MetricName:       MetricHasTriggerMessage,
				MetricValueFloat: asFloat(listener.hasMsg.Load()),
			},
			{
				MetricName:       MetricTopicMetricActive,
				MetricValueFloat: asFloat(listener.lastMetricActive.Load()),
			},
			{
				MetricName:       MetricSubscriptionMetricActive,
				MetricValueFloat: asFloat(m.lastMetricActive.Load()),
			},
			{
				MetricName:       MetricHasPendingMessage,
				MetricValueFloat: asFloat(m.IsActive()),
			},
		},
	}, nil
}
