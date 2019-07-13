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

	t.Parallel()

	nthreads := 4
	var npeers uint8 = 5 // greater than 4 less than 8

	var mask uint64
	for mask = 0; mask < (1<<npeers)-1; mask++ {
		func(mask uint64) {
			tname := fmt.Sprintf("mask[%06b]", mask)
			t.Run(tname, func(t *testing.T) {
				// mode doesn't matter here
				r, err := NewReplicator("replicator", ctx, core.Params{"mode": "each"})
				if err != nil {
					t.Fatalf("failed to initialize replicator: %s", err)
				}
				if err := r.Start(); err != nil {
					t.Fatalf("failed to start replicator: %s", err)
				}
				var lock sync.Mutex
				peers := make([]core.Actor, 0, npeers)
				mailbox := make([][]*core.Message, npeers)

				for i := 0; i < int(npeers); i++ {
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

				msg := core.NewMessage(testutil.RandBytes(1024))
				if err := r.(*Replicator).replicate(msg, mask); err != nil {
					t.Fatalf("failed to send message: %s", err)
				}
				s := msg.Await()
				if s != core.MsgStatusDone {
					t.Fatalf("unexpected msg status: got: %d, want: %d", s, core.MsgStatusDone)
				}

				for i := 0; i < int(npeers); i++ {
					if err := peers[i].Stop(); err != nil {
						t.Fatalf("failed to stop peer: %s", err)
					}
				}
				if err := r.Stop(); err != nil {
					t.Fatalf("failed to stop replicator: %s", err)
				}

				ix := 0
				maskdec := mask
				for maskdec > 0 {
					shouldmatch := (maskdec & 0x1) == 1
					maskdec >>= 1
					if len(mailbox[ix]) == 0 {
						if shouldmatch {
							t.Fatalf("expected to receive a message for ix %d mask %06b, got none: %+v", ix, mask, mailbox)
						}
						ix++
						continue
					}
					lastmsg := mailbox[ix][len(mailbox[ix])-1]
					msgmatch := reflect.DeepEqual(lastmsg.Body(), msg.Body())
					if !(shouldmatch && msgmatch) {
						t.Fatalf("message mismatch: should match: %t, got: %s, want: %s", shouldmatch, lastmsg.Body(), msg.Body())
					}
					ix++
				}
			})
		}(mask)
	}
}

func TestReplicateStatus(t *testing.T) {
	tests := []struct {
		name      string
		statuses  []core.MsgStatus
		expstatus core.MsgStatus
	}{
		{
			name:      "all done",
			statuses:  []core.MsgStatus{core.MsgStatusDone, core.MsgStatusDone, core.MsgStatusDone, core.MsgStatusDone},
			expstatus: core.MsgStatusDone,
		},
		{
			name:      "partial send",
			statuses:  []core.MsgStatus{core.MsgStatusPartialSend, core.MsgStatusDone, core.MsgStatusDone, core.MsgStatusDone},
			expstatus: core.MsgStatusPartialSend,
		},
		{
			name:      "timeout",
			statuses:  []core.MsgStatus{core.MsgStatusTimedOut, core.MsgStatusDone, core.MsgStatusDone, core.MsgStatusDone},
			expstatus: core.MsgStatusTimedOut,
		},
		{
			name:      "other failures",
			statuses:  []core.MsgStatus{core.MsgStatusThrottled, core.MsgStatusFailed, core.MsgStatusInvalid, core.MsgStatusUnroutable},
			expstatus: core.MsgStatusFailed,
		},
		{
			name:      "mixed failures resolved as failure",
			statuses:  []core.MsgStatus{core.MsgStatusDone, core.MsgStatusPartialSend, core.MsgStatusTimedOut, core.MsgStatusFailed},
			expstatus: core.MsgStatusFailed,
		},
		{
			name:      "mixed failures resolved as timeout",
			statuses:  []core.MsgStatus{core.MsgStatusDone, core.MsgStatusPartialSend, core.MsgStatusTimedOut},
			expstatus: core.MsgStatusTimedOut,
		},
		{
			name:      "mixed failures resolved as timeout",
			statuses:  []core.MsgStatus{core.MsgStatusDone, core.MsgStatusPartialSend},
			expstatus: core.MsgStatusPartialSend,
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			repo := cfg.NewRepository()
			ctx, err := core.NewContext(core.NewConfig(repo))
			if err != nil {
				t.Fatalf("failed to create context: %s", err)
			}
			if err := ctx.Start(); err != nil {
				t.Fatalf("failed to start context: %s", err)
			}

			r, err := NewReplicator("replicator", ctx, core.Params{"mode": "each"})
			if err != nil {
				t.Fatalf("failed to create replicator: %s", err)
			}

			npeers := len(testCase.statuses)
			nthreads := 4
			for i := 0; i < npeers; i++ {
				peer, err := flowtest.NewTestActor(
					fmt.Sprintf("test-actor-%d", i),
					ctx,
					core.Params{},
				)
				if err != nil {
					t.Fatalf("failed to create test actor: %s", err)
				}

				func(status core.MsgStatus) {
					peer.(*flowtest.TestActor).OnReceive(func(msg *core.Message) {
						msg.Complete(status)
						peer.(*flowtest.TestActor).Flush()
					})
				}(testCase.statuses[i])

				if err := r.Connect(nthreads, peer); err != nil {
					t.Fatalf("failed to connect test actor: %s", err)
				}
				if err := peer.Start(); err != nil {
					t.Fatalf("failed to start test actor: %s", err)
				}
			}

			if err := r.Start(); err != nil {
				t.Fatalf("failed to start replicator: %s", err)
			}

			msg := core.NewMessage(testutil.RandBytes(1024))
			if err := r.(*Replicator).replicate(msg, (1<<uint8(npeers))-1); err != nil {
				t.Fatalf("failed to send message: %s", err)
			}

			if s := msg.Await(); s != testCase.expstatus {
				t.Fatalf("unexpected message status: got: %d, want: %d", s, testCase.expstatus)
			}
		})
	}
}
