package actor

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/awesome-flow/flow/pkg/cfg"
	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	testutil "github.com/awesome-flow/flow/pkg/util/test"
	flowtest "github.com/awesome-flow/flow/pkg/util/test/corev1alpha1"
)

func TestMaskFanout(t *testing.T) {
	tests := []struct {
		maskin  uint64
		lenq    int
		maskout uint64
	}{
		{0, 0, 0},
		{0, 1, 1},
		{1, 1, 1},
		{2, 1, 1},

		{0, 4, 1},
		{1, 4, 2},
		{2, 4, 4},
		{4, 4, 8},
		{8, 4, 1},

		{0, 3, 1},
		{1, 3, 2},
		{2, 3, 4},
		{4, 3, 1},
	}

	for _, testCase := range tests {
		if maskout := maskFanout(testCase.maskin, testCase.lenq); maskout != testCase.maskout {
			t.Fatalf("unexpected maskFanout value for input {maskin: %d, lenq: %d}: got: %0b, want: %0b", testCase.maskin, testCase.lenq, maskout, testCase.maskout)
		}
	}
}

func TestReplicate(t *testing.T) {
	repo := cfg.NewRepository()
	ctx, err := core.NewContext(core.NewConfig(repo))
	if err != nil {
		t.Fatalf("failed to initialize context: %s", err)
	}
	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}
	// mode doesn't matter here
	r, err := NewReplicator("replicator", ctx, core.Params{"mode": "all"})
	if err != nil {
		t.Fatalf("failed to initialize replicator: %s", err)
	}
	if err := r.Start(); err != nil {
		t.Fatalf("failed to start replicator: %s", err)
	}

	nthreads := 4

	npeers := 6 // greater than 4 less than 8, I should do it in a loop
	var lock sync.Mutex
	peers := make([]core.Actor, 0, npeers)
	mailbox := make([][]*core.Message, npeers)
	for i := 0; i < npeers; i++ {
		mailbox[i] = make([]*core.Message, 0, 1)
		peer, err := flowtest.NewTestActor(
			fmt.Sprintf("test-actor-%d", i),
			ctx,
			core.Params{},
		)
		if err != nil {
			t.Fatalf("failed to initialize test actor: %s", err)
		}
		func(i int) {
			peer.(*flowtest.TestActor).OnReceive(func(msg *core.Message) {
				lock.Lock()
				defer lock.Unlock()
				msg.Complete(core.MsgStatusDone)
				mailbox[i] = append(mailbox[i], msg)
				peer.(*flowtest.TestActor).Flush()
			})
		}(i)

		peers = append(peers, peer)
		if err := peer.Start(); err != nil {
			t.Fatalf("failed to start test actor: %s", err)
		}
		if err := r.Connect(nthreads, peer); err != nil {
			t.Fatalf("failed to connect test actor: %s", err)
		}
	}

	for i := 0; i < npeers; i++ {
		if len(mailbox[i]) != 0 {
			t.Fatalf("dirty mailbox for ix %d", i)
		}
		msg := core.NewMessage(testutil.RandBytes(1024))
		mask := uint64(1 << uint8(i))
		if err := r.(*Replicator).replicate(msg, mask); err != nil {
			t.Fatalf("failed to send message: %s", err)
		}
		s := msg.Await()
		if s != core.MsgStatusDone {
			t.Fatalf("unexpected msg status: got: %d, want: %d", s, core.MsgStatusDone)
		}
		if len(mailbox[i]) == 0 {
			t.Fatalf("empty mailbox for ix: %d", i)
		}
		lastmsg := mailbox[i][len(mailbox[i])-1]
		if !reflect.DeepEqual(lastmsg.Body(), msg.Body()) {
			t.Fatalf("unexpected message in the mailbox: got: %s, want: %s", lastmsg.Body(), msg.Body())
		}
	}
}
