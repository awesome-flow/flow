package link

import (
	"reflect"
	"testing"
	"time"

	"github.com/whiteboxio/flow/pkg/core"
	test "github.com/whiteboxio/flow/pkg/util/test"
)

func TestDemux_multiplex(t *testing.T) {
	tests := []struct {
		descr  string
		links  []core.Link
		expSts core.MsgStatus
	}{
		{
			"succ send",
			test.InitCountAndReplySet(map[string]test.ReplyType{
				"A": test.ReplyDone,
				"B": test.ReplyDone,
				"C": test.ReplyDone,
			}),
			core.MsgStatusDone,
		},
		{
			"part send",
			test.InitCountAndReplySet(map[string]test.ReplyType{
				"A": test.ReplyFailed,
				"B": test.ReplyDone,
				"C": test.ReplyDone,
			}),
			core.MsgStatusPartialSend,
		},
		{
			"fail send",
			test.InitCountAndReplySet(map[string]test.ReplyType{
				"A": test.ReplyFailed,
				"B": test.ReplyFailed,
				"C": test.ReplyFailed,
			}),
			core.MsgStatusFailed,
		},
		{
			"time out",
			test.InitCountAndReplySet(map[string]test.ReplyType{
				"A": test.ReplyDone,
				"B": test.ReplyDone,
				"C": test.ReplyContinue,
			}),
			core.MsgStatusTimedOut,
		},
	}

	for _, tstCase := range tests {
		t.Run(tstCase.descr, func(t *testing.T) {
			demux, err := New("demux", nil, core.NewContext())
			if err != nil {
				t.Errorf("Unexpected demux init error: %s", err.Error())
			}
			linkErr := demux.LinkTo(tstCase.links)
			if linkErr != nil {
				t.Errorf("Failed to connect links to mux: %s", linkErr.Error())
			}
			msg := core.NewMessageWithMeta(
				map[string]interface{}{
					"sync": "true",
				},
				[]byte(""),
			)
			if rcvErr := demux.Recv(msg); rcvErr != nil {
				t.Errorf("Unexpected rcv error: %s", rcvErr.Error())
			}
			select {
			case s := <-msg.GetAckCh():
				if s != tstCase.expSts {
					t.Errorf("Unexpected msg status: %d Vs %d", s, tstCase.expSts)
				}
			case <-time.After(100 * time.Millisecond):
				t.Error("Timed out to receive ack")
			}
		})
	}
}

func Test_Demultiplex(t *testing.T) {
	tests := []struct {
		name           string
		links          []core.Link
		expectedCnts   []int
		expectedStatus core.MsgStatus
		expectedErr    error
	}{
		{
			name: "succ send",
			links: test.InitCountAndReplySet(map[string]test.ReplyType{
				"A": test.ReplyDone,
				"B": test.ReplyDone,
				"C": test.ReplyDone,
			}),
			expectedCnts:   []int{1, 1, 1},
			expectedStatus: core.MsgStatusDone,
			expectedErr:    nil,
		},
		{
			name: "part send",
			links: test.InitCountAndReplySet(map[string]test.ReplyType{
				"A": test.ReplyDone,
				"B": test.ReplyDone,
				"C": test.ReplyFailed,
			}),
			expectedCnts:   []int{1, 1, 1},
			expectedStatus: core.MsgStatusPartialSend,
			expectedErr:    core.ErrMsgPartialSend,
		},
		{
			name: "fail send",
			links: test.InitCountAndReplySet(map[string]test.ReplyType{
				"A": test.ReplyFailed,
				"B": test.ReplyFailed,
				"C": test.ReplyFailed,
			}),
			expectedCnts:   []int{1, 1, 1},
			expectedStatus: core.MsgStatusFailed,
			expectedErr:    core.ErrMsgFailed,
		},
		{
			name: "time out",
			links: test.InitCountAndReplySet(map[string]test.ReplyType{
				"A": test.ReplyDone,
				"B": test.ReplyDone,
				"C": test.ReplyContinue,
			}),
			expectedCnts:   []int{1, 1, 1},
			expectedStatus: core.MsgStatusTimedOut,
			expectedErr:    core.ErrMsgTimedOut,
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			msg := core.NewMessageWithMeta(
				map[string]interface{}{
					"sync": "true",
				},
				[]byte("hello world"),
			)
			err := Demultiplex(msg, DemuxMaskAll, testCase.links, 50*time.Millisecond)
			if !reflect.DeepEqual(err, testCase.expectedErr) {
				t.Errorf("Got an unexpected error: %q, want: %q",
					err, testCase.expectedErr)
			}
			var status core.MsgStatus
			select {
			case status = <-msg.GetAckCh():
				if status != testCase.expectedStatus {
					t.Errorf("Unexpected status from message: %d, want: %d",
						status, testCase.expectedStatus)
				}
			case <-time.After(100 * time.Millisecond):
				t.Errorf("Timed out to receive an ack from message")
			}
			for ix, link := range testCase.links {
				linkRcvCnt := link.(*test.CountAndReply).RcvCnt
				if linkRcvCnt != testCase.expectedCnts[ix] {
					t.Errorf("Unexpected rcv count: %d, want: %d",
						linkRcvCnt, testCase.expectedCnts[ix])
				}
			}
		})
	}
}

// ===== Benchmarks =====

func BenchmarkDemultiplexSync(b *testing.B) {
	links := test.InitCountAndReplySet(map[string]test.ReplyType{
		"A": test.ReplyDone,
		"B": test.ReplyDone,
		"C": test.ReplyDone,
		"D": test.ReplyDone,
	})
	for i := 0; i < b.N; i++ {
		msg := core.NewMessageWithMeta(
			map[string]interface{}{
				core.MsgMetaKeySync: "true",
			},
			test.RandStringBytes(1024),
		)
		if err := Demultiplex(msg, DemuxMaskAll, links, 2*DemuxMsgSendTimeout); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkDemultiplexAsync(b *testing.B) {
	links := test.InitCountAndReplySet(map[string]test.ReplyType{
		"A": test.ReplyDone,
		"B": test.ReplyDone,
		"C": test.ReplyDone,
		"D": test.ReplyDone,
	})
	for i := 0; i < b.N; i++ {
		msg := core.NewMessageWithMeta(
			map[string]interface{}{},
			test.RandStringBytes(1024),
		)
		if err := Demultiplex(msg, DemuxMaskAll, links, 2*DemuxMsgSendTimeout); err != nil {
			b.Error(err)
		}
	}
}
