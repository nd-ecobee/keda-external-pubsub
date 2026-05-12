package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	pb "github.com/kedacore/keda/v2/pkg/scalers/externalscaler"
)

const (
	MetricHasPendingMessage = "has_pending_message"
)

type PubSubScaler struct {
	pb.UnimplementedExternalScalerServer

	podPSClient *pubsub.Client

	listeners   map[string]*TopicListener
	listenersMu sync.RWMutex
}

func NewPubSubScaler() *PubSubScaler {
	ctx := context.Background()
	podPSClient, err := pubsub.NewClient(ctx, pubsub.DetectProjectID)
	if err != nil {
		log.Fatalf("failed to create pubsub client: %v", err)
	}

	return &PubSubScaler{
		podPSClient: podPSClient,
		listeners:   make(map[string]*TopicListener),
	}
}

func (s *PubSubScaler) getListenerWithMeta(meta map[string]string) (*TopicListener, time.Duration, time.Duration, error) {
	topicID := meta["topic"]
	debounceStr := meta["debounceDuration"]
	checkStr := meta["checkInterval"]

	if topicID == "" {
		return nil, 0, 0, fmt.Errorf("topic is required in metadata")
	}

	debounce, err := time.ParseDuration(debounceStr)
	if err != nil {
		debounce = 5 * time.Minute // Default debounce duration 5m
	}

	check, err := time.ParseDuration(checkStr)
	if err != nil {
		check = 1 * time.Minute // Default check interval 1m
	}

	listener, err := s.getListener(topicID)
	if err != nil {
		return nil, 0, 0, err
	}

	return listener, debounce, check, nil
}

func (s *PubSubScaler) IsActive(ctx context.Context, ref *pb.ScaledObjectRef) (*pb.IsActiveResponse, error) {
	log.Printf("RPC IsActive for %s", ref.Name)
	listener, _, _, err := s.getListenerWithMeta(ref.ScalerMetadata)
	if err != nil {
		return nil, err
	}
	active := listener.IsActive()
	log.Printf("IsActive result for %s: %v", ref.Name, active)
	return &pb.IsActiveResponse{Result: active}, nil
}

func (s *PubSubScaler) StreamIsActive(ref *pb.ScaledObjectRef, stream pb.ExternalScaler_StreamIsActiveServer) error {
	log.Printf("RPC StreamIsActive started for %s", ref.Name)
	listener, debounce, check, err := s.getListenerWithMeta(ref.ScalerMetadata)
	if err != nil {
		return err
	}

	ch := make(chan bool, 1)
	listener.Register(ch, debounce, check)

	defer func() {
		listener.Unregister(ch)
		log.Printf("RPC StreamIsActive ended for %s", ref.Name)
	}()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case active := <-ch:
			log.Printf("Streaming active state for %s: %v", ref.Name, active)
			if err := stream.Send(&pb.IsActiveResponse{Result: active}); err != nil {
				return err
			}
		}
	}
}

func (s *PubSubScaler) GetMetricSpec(ctx context.Context, ref *pb.ScaledObjectRef) (*pb.GetMetricSpecResponse, error) {
	log.Printf("RPC GetMetricSpec for %s", ref.Name)
	return &pb.GetMetricSpecResponse{
		MetricSpecs: []*pb.MetricSpec{
			{
				MetricName:      MetricHasPendingMessage,
				TargetSizeFloat: 1,
			},
		},
	}, nil
}

func (s *PubSubScaler) GetMetrics(ctx context.Context, req *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	log.Printf("RPC GetMetrics for %s", req.ScaledObjectRef.Name)
	listener, _, _, err := s.getListenerWithMeta(req.ScaledObjectRef.ScalerMetadata)
	if err != nil {
		return nil, err
	}

	var hasPending float64 = 0
	if listener.IsActive() {
		hasPending = 1
	}

	log.Printf("GetMetrics values for %s: %f", req.ScaledObjectRef.Name, hasPending)
	return &pb.GetMetricsResponse{
		MetricValues: []*pb.MetricValue{
			{
				MetricName:       MetricHasPendingMessage,
				MetricValueFloat: hasPending,
			},
		},
	}, nil
}

func (s *PubSubScaler) getListener(topicID string) (*TopicListener, error) {
	s.listenersMu.RLock()
	if l, ok := s.listeners[topicID]; ok {
		s.listenersMu.RUnlock()
		return l, nil
	}
	s.listenersMu.RUnlock()

	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()

	// Double-check after acquiring write lock
	if l, ok := s.listeners[topicID]; ok {
		return l, nil
	}

	l, err := NewTopicListener(s.podPSClient, topicID)
	if err != nil {
		return nil, err
	}

	s.listeners[topicID] = l
	return l, nil
}
