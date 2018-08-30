package receiver

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/whiteboxio/flow/pkg/core"
)

type A struct {
	lastMsg []byte
	*core.Connector
}

func NewA() *A {
	return &A{nil, core.NewConnector()}
}

func (a *A) Recv(msg *core.Message) error {
	fmt.Printf("A received a message\n")
	a.lastMsg = msg.Payload
	return msg.AckDone()
}

func TestTCP_recv(t *testing.T) {
	tcpAddr := ":7102"
	payload := "hello world\r\n"
	tcp, err := New("test_tcp", core.Params{"bind_addr": tcpAddr})
	if err != nil {
		t.Fatalf("Failed to start a TCP listener: %s", err)
	}
	rcvLink := NewA()
	tcp.ConnectTo(rcvLink)
	conn, connErr := net.DialTimeout("tcp", tcpAddr, 1*time.Second)
	if connErr != nil {
		t.Fatalf("Failed to open a TCP connection: %s", connErr)
	}
	n, writeErr := conn.Write([]byte(payload))
	if writeErr != nil {
		t.Fatalf("Failed to write TCP data: %s", writeErr)
	}
	t.Logf("Sent %d bytes over the network", n)
	time.Sleep(10 * time.Millisecond)
	if string(rcvLink.lastMsg) != strings.Trim(payload, "\n\r") {
		t.Fatalf("Unexpected receiver last message: %s", rcvLink.lastMsg)
	}
}
