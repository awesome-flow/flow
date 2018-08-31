DOCKER_IMAGE_NAME=flow/tcp_server
BIN_NAME=tcp_server

build:
	@go build -v -o $(BIN_NAME)
clean:
	rm -f $(BIN_NAME)
	rm -f $(BIN_NAME)_linux
build-docker:
	@GOOS=linux GOARCH=386 go build -a -o $(BIN_NAME)_linux .
build-docker-image:
	@docker build -t $(DOCKER_IMAGE_NAME) .
