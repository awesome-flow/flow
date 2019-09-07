BINARY_NAME=flowd
DOCKER_COMPOSE=docker-compose
GOCMD=go
GOVERSION=1.13
GO111MODULE=on
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
MAKE=make

test:
	@$(GOTEST) -race -v ./...

build:
	@cd cmd/flowd-v1alpha1; $(GOBUILD) -o ../../builds/$(BINARY_NAME) -v -trimpath
