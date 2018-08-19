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

build-docker-images:
	@cd internal/app/tcp_server/ && $(MAKE) -f Makefile build-docker && $(MAKE) -f Makefile build-docker-image && $(MAKE) -f Makefile clean
	@cd internal/app/udp_server/ && $(MAKE) -f Makefile build-docker && $(MAKE) -f Makefile build-docker-image && $(MAKE) -f Makefile clean