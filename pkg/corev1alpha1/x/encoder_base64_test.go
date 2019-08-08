package x

import (
	"encoding/base64"
	"reflect"
	"sync"
	"testing"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	coretest "github.com/awesome-flow/flow/pkg/corev1alpha1/test"
	"github.com/awesome-flow/flow/pkg/util"
	testutil "github.com/awesome-flow/flow/pkg/util/test"
	flowtest "github.com/awesome-flow/flow/pkg/util/test/corev1alpha1"
)

func TestEncoderBase64Receive(t *testing.T) {
	nthreads := 4
	ctx, err := coretest.NewContextWithConfig(map[string]interface{}{})
	if err != nil {
		t.Fatalf("failed to create a context: %s", err)
	}
	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}
	defer ctx.Stop()

	enc, err := NewEncoderBase64("encoder", ctx, core.Params{})
	if err != nil {
		t.Fatalf("failed to create a new encoder: %s", err)
	}
	act, err := flowtest.NewTestActor("test-actor", ctx, core.Params{})
	if err != nil {
		t.Fatalf("failed to create a new test actor: %s", err)
	}
	if err := enc.Connect(nthreads, act); err != nil {
		t.Fatalf("failed to connect test actor to encoder: %s", err)
	}
	mailbox := make(chan *core.Message, 1)
	defer close(mailbox)

	var lock sync.Mutex
	act.(*flowtest.TestActor).OnReceive(func(msg *core.Message) {
		lock.Lock()
		defer lock.Unlock()
		act.(*flowtest.TestActor).Flush()
		mailbox <- msg
		msg.Complete(core.MsgStatusDone)
	})

	if err := util.ExecEnsure(
		act.Start,
		enc.Start,
	); err != nil {
		t.Fatalf("failed to start actor: %s", err)
	}

	defer util.ExecEnsure(
		enc.Stop,
		act.Stop,
	)

	data := testutil.RandBytes(testutil.RandInt(1024))
	datalen := len(data)
	msg := core.NewMessage(data)

	encoding := base64.StdEncoding
	explen := encoding.EncodedLen(datalen)
	expdata := make([]byte, explen)
	encoding.Encode(expdata, data)

	if err := enc.Receive(msg); err != nil {
		t.Fatalf("failed to send a message: %s", err)
	}

	if status := msg.Await(); status != core.MsgStatusDone {
		t.Fatalf("unexpected message status: got: %d, want: %d", status, core.MsgStatusDone)
	}
	gotmsg := <-mailbox
	if !reflect.DeepEqual(expdata, gotmsg.Body()) {
		t.Fatalf("unexpected message data: got: %q, want: %q", gotmsg.Body(), expdata)
	}
}
