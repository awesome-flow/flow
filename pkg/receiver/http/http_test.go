package receiver

import (
	"bytes"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/whiteboxio/flow/pkg/core"
)

type Receiver struct {
	lastMessage *core.Message
	behavior    uint32
	*core.Connector
}

const (
	RcvDone uint32 = iota
	RcvPartial
	RcvInvalid
	RcvFailed
	RcvTimeout
	RcvUnroutable
	RcvThrottled
)

func NewReceiver(behavior uint32) *Receiver {
	return &Receiver{nil, behavior, core.NewConnector()}
}

func (rcv *Receiver) Recv(msg *core.Message) error {
	rcv.lastMessage = msg
	switch rcv.behavior {
	case RcvDone:
		return msg.AckDone()
	case RcvPartial:
		return msg.AckPartialSend()
	case RcvInvalid:
		return msg.AckInvalid()
	case RcvFailed:
		return msg.AckFailed()
	case RcvTimeout:
		return msg.AckTimedOut()
	case RcvUnroutable:
		return msg.AckUnroutable()
	case RcvThrottled:
		return msg.AckThrottled()
	}
	return fmt.Errorf("Unknown message status")
}

func TestHTTP_handleSendV1(t *testing.T) {
	tests := []struct {
		name               string
		bindAddr           string
		behavior           uint32
		payload            []byte
		extra              string
		expectedStatusCode int
		expectedPayload    []byte
		expectedMeta       core.MsgMeta
	}{
		{
			"async successful",
			":7101",
			RcvDone,
			[]byte("hello world"),
			"",
			http.StatusAccepted,
			[]byte("hello world"),
			core.MsgMeta{},
		},
		{
			"sync successful",
			":7102",
			RcvDone,
			[]byte("hello world"),
			"?sync=true",
			http.StatusOK,
			[]byte("hello world"),
			core.MsgMeta{"sync": "true"},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			httpRcv, err := NewHTTP("test_http",
				core.Params{"bind_addr": testCase.bindAddr})
			if err != nil {
				t.Errorf("Failed to create an HTTP receiver: %s", err)
			}
			rcv := NewReceiver(testCase.behavior)
			httpRcv.ConnectTo(rcv)
			time.Sleep(10 * time.Millisecond)
			buf := bytes.NewReader(testCase.payload)
			resp, err := http.Post(
				"http://localhost:7101/api/v1/send"+testCase.extra,
				"application/octet-stream",
				buf,
			)
			if err != nil {
				t.Errorf("Failed to send an HTTP request: %s", err)
			}
			if resp.StatusCode != testCase.expectedStatusCode {
				t.Errorf("Unexpected HTTP status code: %d, want: %d",
					resp.StatusCode, testCase.expectedStatusCode)
			}
			time.Sleep(10 * time.Millisecond)
			if rcv.lastMessage == nil {
				t.Fatalf("Receiver message is nil")
			}
			if bytes.Compare(testCase.expectedPayload, rcv.lastMessage.Payload) == 0 {
				t.Errorf("Unexpected last message content: %s, want: %s",
					rcv.lastMessage.Payload, testCase.expectedPayload)
			}
			if !reflect.DeepEqual(rcv.lastMessage.Meta, testCase.expectedMeta) {
				t.Errorf("Unexpected message meta: %+v, want: %+v",
					rcv.lastMessage.Meta, testCase.expectedMeta)
			}
		})
	}
}
