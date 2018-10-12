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

func TestUDP_recv(t *testing.T) {
	udpAddr := ":7001"
	payload := "hello world\r\n"
	udp, err := New("test_udp", core.Params{"bind_addr": udpAddr}, core.NewContext())
	if err != nil {
		t.Fatalf("Failed to start a UDP listener: %s", err.Error())
	}
	rcvLink := NewA()
	udp.ConnectTo(rcvLink)
	conn, connErr := net.DialTimeout("udp", udpAddr, 1*time.Second)
	if connErr != nil {
		t.Fatalf("Failed to open a udp connection: %s", connErr.Error())
	}
	conn.SetDeadline(time.Now().Add(time.Second))
	n, writeErr := conn.Write([]byte(payload))
	if writeErr != nil {
		t.Fatalf("Failed to write udp data: %s", writeErr.Error())
	}
	t.Logf("Sent %d bytes over the network", n)
	time.Sleep(10 * time.Millisecond)
	if string(rcvLink.lastMsg) != strings.Trim(payload, "\r\n") {
		t.Fatalf("Unexpected receiver last message: %s", rcvLink.lastMsg)
	}
}
