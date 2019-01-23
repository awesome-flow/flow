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

func TestTCP_recv(t *testing.T) {
	tcpAddr := ":7102"

	newline := []byte{'\r', '\n'}
	payload := testutils.RandStringBytes(DefaultMessageSize)

	tcp, err := New("test_tcp", core.Params{"bind_addr": tcpAddr}, core.NewContext())
	if err != nil {
		t.Fatalf("Failed to initialize a TCP listener: %s", err)
	}
	if err := tcp.ExecCmd(core.NewCmdStart()); err != nil {
		t.Fatalf("Failed to start the TCP listener: %s", err)
	}
	rcvLink := testutils.NewRememberAndReply("rar", testutils.ReplyDone)
	tcp.ConnectTo(rcvLink)

	conn, connErr := net.DialTimeout("tcp", tcpAddr, 1*time.Second)
	if connErr != nil {
		t.Fatalf("Failed to open a TCP connection: %s", connErr)
	}

	_, writeErr := conn.Write(append(payload, newline...))
	if writeErr != nil {
		t.Fatalf("Failed to write TCP data: %s", writeErr)
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
		t.Fatalf("Unexpected receiver last message: got %q, want: %q",
			rcvLink.LastMsg().Payload(), payload)
	}
}
