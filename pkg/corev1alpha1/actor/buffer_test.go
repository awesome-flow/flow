package actor

import (
	"reflect"
	"testing"

	"github.com/awesome-flow/flow/pkg/cfg"
	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/types"
	"github.com/awesome-flow/flow/pkg/util"
	testutil "github.com/awesome-flow/flow/pkg/util/test"
	flowtest "github.com/awesome-flow/flow/pkg/util/test/corev1alpha1"
)

func TestSimplePipe(t *testing.T) {
	repo := cfg.NewRepository()

	nthreads := 1 + testutil.RandInt(4)

	if _, err := cfg.NewScalarConfigProvider(
		&types.KeyValue{
			Key:   types.NewKey("system.maxprocs"),
			Value: nthreads,
		},
		repo,
		42, // doesn't matter
	); err != nil {
		t.Fatalf("failed to create a new scalar provider: %s", err)
	}

	ctx, err := core.NewContext(core.NewConfig(repo))
	if err != nil {
		t.Fatalf("failed to create a new context: %s", err)
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
	if err := buf.Connect(nthreads, act); err != nil {
		t.Fatalf("failed to connect test actor to buf: %s", err)
	}

	mailbox := make(chan *core.Message, 1)

	act.(*flowtest.TestActor).OnReceive(func(msg *core.Message) {
		msg.Complete(core.MsgStatusDone)
		mailbox <- msg
		act.(*flowtest.TestActor).Flush()
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
	msg.Await()

	if len(mailbox) != 1 {
		t.Fatalf("unexpected length of the mailbox: got: %d, want: %d", len(mailbox), 1)
	}

	rcvmsg := <-mailbox
	if !reflect.DeepEqual(rcvmsg.Body(), msg.Body()) {
		t.Fatalf("unexpected contents of the mailbox: got: %+v, want: %+v", rcvmsg, msg)
	}
}
