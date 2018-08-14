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

func TestHTTP_handleSendV1_AckDone(t *testing.T) {
	httpRcv, err := NewHTTP("test_http", core.Params{"bind_addr": ":7101"})
	if err != nil {
		t.Fatalf("Failed to initialize HTTP receiver: %s", err)
	}
	rcv := NewReceiver(RcvDone)
	httpRcv.ConnectTo(rcv)
	time.Sleep(10 * time.Millisecond)
	payload := "hello world"
	buf := bytes.NewReader([]byte(payload))
	resp, err := http.Post(
		"http://localhost:7101/api/v1/send",
		"application/json",
		buf,
	)
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("Failed to execute an HTTP request: %s", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("Unexpected status code: %d", resp.StatusCode)
	}
	_, readErr := ioutil.ReadAll(resp.Body)
	if readErr != nil {
		t.Fatalf("Failed to read the response body: %s", readErr)
	}
	if string(rcv.lastMessage.Payload) != payload {
		t.Fatalf("Unexpected receiver last message contents: %s",
			rcv.lastMessage.Payload)
	}
}
