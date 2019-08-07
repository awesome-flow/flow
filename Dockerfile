FROM golang:1.12-alpine
RUN apk add build-base
RUN apk add git
WORKDIR /go/src/github.com/awesome-flow/flow/
ADD . .
RUN go get -u github.com/golang/dep/cmd/dep
RUN dep ensure
RUN make build
ENTRYPOINT ["sh", "./docker/flowd-runner.sh"]
