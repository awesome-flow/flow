package actor

import (
	"reflect"
	"testing"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	coretest "github.com/awesome-flow/flow/pkg/corev1alpha1/test"
	testutil "github.com/awesome-flow/flow/pkg/util/test"
	flowtest "github.com/awesome-flow/flow/pkg/util/test/corev1alpha1"
)

func TestUDPHandleConn(t *testing.T) {
	nthreads := 4
	ctx, err := coretest.NewContextWithConfig(map[string]interface{}{
		"system.maxprocs": nthreads,
	})
	if err != nil {
		t.Fatalf("failed to create context: %d", err)
	}

	rcv, err := NewReceiverUDP("receiver", ctx, core.Params{
		"bind": "127.0.0.1:12345",
	})
	if err != nil {
		t.Fatalf("failed to create UDP receiver: %s", err)
	}

	peer, err := flowtest.NewTestActor("test-actor", ctx, core.Params{})
	if err != nil {
		t.Fatalf("failed to create test actor: %s", err)
	}
	mailbox := make(chan *core.Message)
	peer.(*flowtest.TestActor).OnReceive(func(msg *core.Message) {
		msg.Complete(core.MsgStatusDone)
		mailbox <- msg
		peer.(*flowtest.TestActor).Flush()
	})

	if err := rcv.Connect(nthreads, peer); err != nil {
		t.Fatalf("failed to connect test actor: %s", err)
	}

	conn := newTestConn(
		newTestAddr("tcp", "127.0.0.1:12345"),
		newTestAddr("tcp", "127.0.0.1:23456"),
	)
	body := testutil.RandBytes(1024)
	if _, err := conn.Write(body); err != nil {
		t.Fatalf("failed to write body data to test conn: %s", err)
	}

	rcv.(*ReceiverUDP).handleConn(conn)
	msg := <-mailbox

	if !reflect.DeepEqual(msg.Body(), body) {
		t.Fatalf("unexpected mesage body: got: %s, want: %s", msg.Body(), body)
	}
}
