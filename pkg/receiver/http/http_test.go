package receiver

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/whiteboxio/flow/pkg/core"
)

type Receiver struct {
	lastMessage []byte
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
	return &Receiver{[]byte{}, behavior, core.NewConnector()}
}

func (rcv *Receiver) Recv(msg *core.Message) error {
	rcv.lastMessage = msg.Payload

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
			"sync empty",
			":17101",
			RcvDone,
			[]byte{},
			"",
			http.StatusBadRequest,
			[]byte{},
			core.MsgMeta{},
		},
		{
			"async successful",
			":17101",
			RcvDone,
			[]byte("hello world"),
			"",
			http.StatusAccepted,
			[]byte("hello world"),
			core.MsgMeta{},
		},
		{
			"sync successful",
			":17101",
			RcvDone,
			[]byte("hello world"),
			"?sync=true",
			http.StatusOK,
			[]byte("hello world"),
			core.MsgMeta{"sync": "true"},
		},
		{
			"sync partial",
			":17101",
			RcvPartial,
			[]byte("hello world"),
			"?sync=true",
			http.StatusConflict,
			[]byte("hello world"),
			core.MsgMeta{"sync": "true"},
		},
		{
			"sync invalid",
			":17101",
			RcvInvalid,
			[]byte("hello world"),
			"?sync=true",
			http.StatusBadRequest,
			[]byte("hello world"),
			core.MsgMeta{"sync": "true"},
		},
		{
			"sync failed",
			":17101",
			RcvFailed,
			[]byte("hello world"),
			"?sync=true",
			http.StatusInternalServerError,
			[]byte("hello world"),
			core.MsgMeta{"sync": "true"},
		},
		{
			"sync timeout",
			":17101",
			RcvTimeout,
			[]byte("hello world"),
			"?sync=true",
			http.StatusGatewayTimeout,
			[]byte("hello world"),
			core.MsgMeta{"sync": "true"},
		},
		{
			"sync unroutable",
			":17101",
			RcvUnroutable,
			[]byte("hello world"),
			"?sync=true",
			http.StatusNotAcceptable,
			[]byte("hello world"),
			core.MsgMeta{"sync": "true"},
		},
		{
			"sync throttled",
			":17101",
			RcvThrottled,
			[]byte("hello world"),
			"?sync=true",
			http.StatusTooManyRequests,
			[]byte("hello world"),
			core.MsgMeta{"sync": "true"},
		},
	}

	for _, testCase := range tests {
		t.Logf("Inspecting case %s", testCase.name)
		httpRcv, err := NewHTTP("test_http",
			core.Params{"bind_addr": testCase.bindAddr})
		if err != nil {
			t.Errorf("Failed to create an HTTP receiver: %s", err)
		}
		time.Sleep(10 * time.Millisecond)
		rcv := NewReceiver(testCase.behavior)
		httpRcv.ConnectTo(rcv)
		buf := bytes.NewReader(testCase.payload)
		resp, err := http.Post(
			"http://"+testCase.bindAddr+"/send"+testCase.extra,
			"application/octet-stream",
			buf,
		)
		if err != nil {
			t.Errorf("Failed to send an HTTP request: %s", err)
		}
		if resp == nil {
			t.Fatalf("Response is nil")
		}
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Errorf("Failed to read the body: %s", err)
		}
		t.Logf("Body: %s", body)
		if resp.StatusCode != testCase.expectedStatusCode {
			t.Errorf("Unexpected HTTP status code: %d, want: %d",
				resp.StatusCode, testCase.expectedStatusCode)
		}
		if bytes.Compare(testCase.expectedPayload, rcv.lastMessage) != 0 {
			t.Errorf("Unexpected last message content: %s, want: %s",
				rcv.lastMessage, testCase.expectedPayload)
		}
		httpRcv.ExecCmd(&core.Cmd{Code: core.CmdCodeStop})
	}
}
