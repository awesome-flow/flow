package actor

import (
	"fmt"
	"reflect"
	"testing"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	coretest "github.com/awesome-flow/flow/pkg/corev1alpha1/test"
	testutil "github.com/awesome-flow/flow/pkg/util/test"
	flowtest "github.com/awesome-flow/flow/pkg/util/test/corev1alpha1"
)

func TestTCPHandleConn(t *testing.T) {
	nthreads := 4
	ctx, err := coretest.NewContextWithConfig(map[string]interface{}{})
	if err != nil {
		t.Fatalf("failed to create context: %s", err)
	}
	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}

	type testStruct struct {
		name    string
		status  core.MsgStatus
		sync    bool
		expresp []byte
	}

	tests := make([]testStruct, 0, len(MsgStatusToTcpResp))
	for status, resp := range MsgStatusToTcpResp {
		tests = append(tests, testStruct{
			name:    fmt.Sprintf("response with %s", sts2name(status)),
			status:  status,
			sync:    true,
			expresp: resp,
		})
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			conn := newTestConn(
				newTestAddr("tcp", "127.0.0.1:12345"),
				newTestAddr("tcp", "127.0.0.1:23456"),
			)
			params := core.Params{
				"bind": "127.0.0.1:12345",
				"sync": testCase.sync,
			}
			rcv, err := NewReceiverTCP("receiver", ctx, params)
			if err != nil {
				t.Fatalf("failed to create receiver: %s", err)
			}

			peer, err := flowtest.NewTestActor("test-actor", ctx, core.Params{})
			if err != nil {
				t.Fatalf("failed to create test actor: %s", err)
			}
			if err := rcv.Connect(nthreads, peer); err != nil {
				t.Fatalf("failed to connect test receiver: %s", err)
			}

			mailbox := make(chan *core.Message, 1)
			defer close(mailbox)

			peer.(*flowtest.TestActor).OnReceive(func(msg *core.Message) {
				mailbox <- msg
				msg.Complete(testCase.status)
				peer.(*flowtest.TestActor).Flush()
			})

			body := testutil.RandBytes(8 * 1024)
			if _, err := conn.Write(body); err != nil {
				t.Fatalf("failed to write body data to test conn: %s", err)
			}

			rcv.(*ReceiverTCP).handleConn(conn)
			if !reflect.DeepEqual(conn.buf, testCase.expresp) {
				t.Fatalf("unexpected conn buf: got: %s, want: %s", string(conn.buf), testCase.expresp)
			}

			msg := <-mailbox
			if !reflect.DeepEqual(msg.Body(), body) {
				t.Fatalf("unexpected message body: got: %s, want: %s", string(msg.Body()), string(body))
			}
		})
	}

}
