package receiver

import (
	"net"
	"os"
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
	a.lastMsg = msg.Payload
	return msg.AckDone()
}

func TestUnix_unixRecv(t *testing.T) {
	path := "/tmp/flow.sock"
	defer os.Remove(path)
	testRcv := NewA()
	payload := "hello world" + "\n"
	unix, err := New("test_unix", core.Params{"path": path}, core.NewContext())
	if err != nil {
		t.Fatalf("Failed to initialize unix receiver: %s", err.Error())
	}
	unix.ConnectTo(testRcv)
	time.Sleep(10 * time.Millisecond)
	conn, connErr := net.Dial("unix", path)
	if connErr != nil {
		t.Fatalf("Unable to connect to the unix socket: %s", connErr.Error())
	}
	if _, err := conn.Write([]byte(payload)); err != nil {
		t.Fatalf("Unable to write data to unix socket: %s", err.Error())
	}
	time.Sleep(10 * time.Millisecond)
	if string(testRcv.lastMsg) != payload {
		t.Fatalf("Unexpected contents in receiver last message: %s", testRcv.lastMsg)
	}
}
