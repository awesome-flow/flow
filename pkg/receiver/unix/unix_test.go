package receiver

import (
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/awesome-flow/flow/pkg/core"
	testutils "github.com/awesome-flow/flow/pkg/util/test"
)

const (
	DefaultMessageSize = 1024
)

func TestUnix_unixRecv(t *testing.T) {
	path := "/tmp/flow.sock"
	defer os.Remove(path)
	testRcv := testutils.NewRememberAndReply("rar", testutils.ReplyDone)
	payload := append(testutils.RandStringBytes(DefaultMessageSize), '\n')
	unix, err := New("test_unix", core.Params{"path": path}, core.NewContext())
	if err != nil {
		t.Fatalf("Failed to initialize unix receiver: %s", err.Error())
	}

	if err := unix.ExecCmd(core.NewCmdStart()); err != nil {
		t.Fatalf("Failed to start unix link: %s", err)
	}

	unix.ConnectTo(testRcv)

	conn, connErr := net.Dial("unix", path)
	if connErr != nil {
		t.Fatalf("Unable to connect to the unix socket: %s", connErr.Error())
	}

	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("Unable to write data to unix socket: %s", err.Error())
	}

	received := make(chan struct{})
	go func() {
		for {
			if testRcv.LastMsg() != nil {
				received <- struct{}{}
			}
		}
	}()

	select {
	case <-received:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("timed out to receive the message")
	}

	if !reflect.DeepEqual(testRcv.LastMsg().Payload(), payload) {
		t.Fatalf("Unexpected contents in receiver last message: %s", testRcv.LastMsg().Payload())
	}
}
