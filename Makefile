BINARY_NAME=flowd
DOCKER_COMPOSE=docker-compose
GOCMD=GO111MODULE=on go
GOVERSION=1.12
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
MAKE=make

test:
	@$(GOTEST) -race -v ./...

build:
	@cd cmd/flowd-v1alpha1; $(GOBUILD) -o ../../builds/$(BINARY_NAME) -v
