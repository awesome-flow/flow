BINARY_NAME=flowd
DOCKER_COMPOSE=docker-compose
GOCMD=go
GOVERSION=1.10
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
MAKE=make

test: 
	@$(GOTEST) -v ./...

build:
	@cd cmd/flowd; $(GOBUILD) -o ../../$(BINARY_NAME) -v