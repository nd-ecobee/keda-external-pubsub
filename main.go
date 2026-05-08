package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "github.com/kedacore/keda/v2/pkg/scalers/externalscaler"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "9090"
	}

	address := fmt.Sprintf(":%s", port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption

	certPath := os.Getenv("TLS_CERT_PATH")
	keyPath := os.Getenv("TLS_KEY_PATH")
	caPath := os.Getenv("TLS_CA_PATH")

	if certPath != "" && keyPath != "" && caPath != "" {
		log.Printf("Enabling mTLS with cert: %s, key: %s, ca: %s", certPath, keyPath, caPath)
		serverCert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			log.Fatalf("failed to load x509 key pair: %v", err)
		}

		caCert, err := os.ReadFile(caPath)
		if err != nil {
			log.Fatalf("failed to read ca cert: %v", err)
		}

		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
			log.Fatalf("failed to append ca certs")
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			ClientCAs:    caCertPool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
			MinVersion:   tls.VersionTLS12,
		}

		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	} else {
		log.Println("Starting insecure gRPC server (no TLS env vars set)")
	}

	grpcServer := grpc.NewServer(opts...)
	scaler := NewPubSubScaler()
	pb.RegisterExternalScalerServer(grpcServer, scaler)

	log.Printf("Starting KEDA External Pub/Sub Scaler on %s", address)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
