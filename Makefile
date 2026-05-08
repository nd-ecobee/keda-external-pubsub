# Makefile for building the KEDA External Pub/Sub Scaler

APP_NAME ?= keda-external-pubsub

.PHONY: build
build:
	pack build $(APP_NAME) \
		--descriptor project.toml

.PHONY: clean
clean:
	rm -f keda-external-pubsub
