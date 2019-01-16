package receiver

import (
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/awesome-flow/flow/pkg/core"
	testutils "github.com/awesome-flow/flow/pkg/util/test"
)

const (
	DefaultMessageSize = 1024
)

func TestUDP_recv(t *testing.T) {
	udpAddr := ":7001"

	newline := []byte{'\r', '\n'}
	payload := testutils.RandStringBytes(DefaultMessageSize)

	udp, err := New("test_udp", core.Params{"bind_addr": udpAddr}, core.NewContext())
	if err != nil {
		t.Fatalf("Failed to start a UDP listener: %s", err.Error())
	}
	rcvLink := testutils.NewRememberAndReply("rar", testutils.ReplyDone)

	udp.ConnectTo(rcvLink)
	conn, connErr := net.DialTimeout("udp", udpAddr, 1*time.Second)
	if connErr != nil {
		t.Fatalf("Failed to open a udp connection: %s", connErr.Error())
	}

	conn.SetDeadline(time.Now().Add(time.Second))

	_, writeErr := conn.Write(append(payload, newline...))
	if writeErr != nil {
		t.Fatalf("Failed to write udp data: %s", writeErr.Error())
	}

	received := make(chan struct{})
	go func() {
		for {
			if rcvLink.LastMsg() != nil {
				received <- struct{}{}
				return
			}
		}
	}()

	select {
	case <-received:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Timed out to receive the message")
	}

	if !reflect.DeepEqual(rcvLink.LastMsg().Payload(), payload) {
		t.Fatalf("Unexpected receiver last message: %s", rcvLink.LastMsg().Payload())
	}
}
