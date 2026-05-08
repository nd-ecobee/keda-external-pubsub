# Makefile for building the KEDA External Pub/Sub Scaler

PLATFORM ?= linux/amd64
CN ?= keda-external-pubsub

.PHONY: build
build:
	pack build $(CN) \
		--descriptor project.toml \
		--platform $(PLATFORM)

.PHONY: publish
publish:
	pack build $(CN) \
		--descriptor project.toml \
		--platform $(PLATFORM) \
		--publish

.PHONY: clean
clean:
	rm -f keda-external-pubsub
