package actor

import (
	"reflect"
	"sync"
	"testing"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	coretest "github.com/awesome-flow/flow/pkg/corev1alpha1/test"
	"github.com/awesome-flow/flow/pkg/types"
	"github.com/awesome-flow/flow/pkg/util"
	testutil "github.com/awesome-flow/flow/pkg/util/test"
	flowtest "github.com/awesome-flow/flow/pkg/util/test/corev1alpha1"
)

func TestBufferRetry(t *testing.T) {
	tests := []struct {
		name      string
		counts    []uint32
		statuses  []core.MsgStatus
		expcnt    int
		expstatus core.MsgStatus
	}{
		{
			name:      "instant done",
			counts:    []uint32{0},
			statuses:  []core.MsgStatus{core.MsgStatusDone},
			expcnt:    1,
			expstatus: core.MsgStatusDone,
		},
		{
			name:      "partial send",
			counts:    []uint32{0},
			statuses:  []core.MsgStatus{core.MsgStatusPartialSend},
			expcnt:    1,
			expstatus: core.MsgStatusPartialSend,
		},
		{
			name:      "fail first and done",
			counts:    []uint32{0, 1},
			statuses:  []core.MsgStatus{core.MsgStatusFailed, core.MsgStatusDone},
			expcnt:    2,
			expstatus: core.MsgStatusDone,
		},
		{
			name:      "fails maxAttempts-1 times",
			counts:    []uint32{0, DefaultBufMaxAttempts - 1},
			statuses:  []core.MsgStatus{core.MsgStatusFailed, core.MsgStatusDone},
			expcnt:    DefaultBufMaxAttempts,
			expstatus: core.MsgStatusDone,
		},
		{
			name:      "fails maxAttempts times",
			counts:    []uint32{0, DefaultBufMaxAttempts},
			statuses:  []core.MsgStatus{core.MsgStatusFailed, core.MsgStatusDone},
			expcnt:    DefaultBufMaxAttempts,
			expstatus: core.MsgStatusFailed,
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, err := coretest.NewContextWithConfig(map[string]interface{}{"system.maxprocs": 1 + testutil.RandInt(4)})
			if err != nil {
				t.Fatalf("failed to create a context: %s", err)
			}
			if err := ctx.Start(); err != nil {
				t.Fatalf("failed to start context: %s", err)
			}
			defer ctx.Stop()

			buf, err := NewBuffer("buffer", ctx, core.Params(nil))
			if err != nil {
				t.Fatalf("failed to create a new buffer: %s", err)
			}
			act, err := flowtest.NewTestActor("test-actor", ctx, core.Params(nil))
			if err != nil {
				t.Fatalf("failed to create a new test actor: %s", err)
			}
			nthreads, ok := ctx.Config().Get(types.NewKey("system.maxprocs"))
			if !ok {
				t.Fatalf("failed to fetch `system.maxprocs` config")
			}
			if err := buf.Connect(nthreads.(int), act); err != nil {
				t.Fatalf("failed to connect test actor to buf: %s", err)
			}

			mailbox := make(chan *core.Message, testCase.expcnt)
			defer close(mailbox)

			var cnt uint32
			var lock sync.Mutex
			act.(*flowtest.TestActor).OnReceive(func(msg *core.Message) {
				lock.Lock()
				defer lock.Unlock()
				act.(*flowtest.TestActor).Flush()
				mailbox <- msg
				var lastix int
				for ix, v := range testCase.counts {
					if v > cnt {
						break
					}
					lastix = ix
				}
				msg.Complete(testCase.statuses[lastix])
				cnt++
			})

			if err := util.ExecEnsure(
				act.Start,
				buf.Start,
			); err != nil {
				t.Fatalf("failed to start actor: %s", err)
			}

			defer util.ExecEnsure(
				buf.Stop,
				act.Stop,
			)

			msg := core.NewMessage(testutil.RandBytes(1024))
			if err := buf.Receive(msg); err != nil {
				t.Fatalf("buffer failed to receive a message: %s", err)
			}

			status := msg.Await()
			if status != testCase.expstatus {
				t.Fatalf("unexpected status: got: %d, want: %d", status, testCase.expstatus)
			}

			if len(mailbox) != testCase.expcnt {
				t.Fatalf("unexpected length of the mailbox: got: %d, want: %d", len(mailbox), testCase.expcnt)
			}

			for i := 0; i < testCase.expcnt; i++ {
				rcvmsg := <-mailbox
				if !reflect.DeepEqual(rcvmsg.Body(), msg.Body()) {
					t.Fatalf("unexpected contents of the mailbox: got: %+v, want: %+v", rcvmsg, msg)
				}
			}
		})
	}
}
