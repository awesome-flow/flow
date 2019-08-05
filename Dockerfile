FROM golang:1.12-alpine
RUN apk add build-base
WORKDIR /go/src/github.com/awesome-flow/flow/
ADD . .
RUN make build
ENTRYPOINT ["./builds/flowd"]
