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
		podPSClient: podPSClient,
		listeners:   make(map[string]*TopicListener),
	}
}

func (s *PubSubScaler) getListenerWithMeta(meta map[string]string) (*TopicListener, time.Duration, time.Duration, error) {
	topicID := meta["topic"]
	holdStr := meta["holdDuration"]
	checkStr := meta["checkInterval"]

	if topicID == "" {
		return nil, 0, 0, fmt.Errorf("topic is required in metadata")
	}

	hold, err := time.ParseDuration(holdStr)
	if err != nil {
		hold = 5 * time.Minute // Default hold duration 5m
	}

	check, err := time.ParseDuration(checkStr)
	if err != nil {
		check = 1 * time.Minute // Default check interval 1m
	}

	listener, err := s.getListener(topicID)
	if err != nil {
		return nil, 0, 0, err
	}

	return listener, hold, check, nil
}

func splitGCPResource(res string) []string {
	return strings.Split(res, "/")
}

func (s *PubSubScaler) IsActive(ctx context.Context, ref *pb.ScaledObjectRef) (*pb.IsActiveResponse, error) {
	listener, _, _, err := s.getListenerWithMeta(ref.ScalerMetadata)
	if err != nil {
		return nil, err
	}
	return &pb.IsActiveResponse{Result: listener.IsActive()}, nil
}

func (s *PubSubScaler) StreamIsActive(ref *pb.ScaledObjectRef, stream pb.ExternalScaler_StreamIsActiveServer) error {
	listener, hold, check, err := s.getListenerWithMeta(ref.ScalerMetadata)
	if err != nil {
		return err
	}

	ch := make(chan bool, 1)
	listener.Register(ch, hold, check)

	defer func() {
		listener.Unregister(ch)
	}()

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
				MetricName:      MetricHasPendingMessage,
				TargetSizeFloat: 1,
			},
		},
	}, nil
}

func (s *PubSubScaler) GetMetrics(ctx context.Context, req *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	listener, _, _, err := s.getListenerWithMeta(req.ScaledObjectRef.ScalerMetadata)
	if err != nil {
		return nil, err
	}

	var hasPending float64 = 0
	if listener.IsActive() {
		hasPending = 1
	}

	return &pb.GetMetricsResponse{
		MetricValues: []*pb.MetricValue{
			{
				MetricName:       MetricHasPendingMessage,
				MetricValueFloat: hasPending,
			},
		},
	}, nil
}
