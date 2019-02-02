package link

import (
	"reflect"
	"testing"
	"time"

	"github.com/awesome-flow/flow/pkg/core"
	core_test "github.com/awesome-flow/flow/pkg/util/core_test"
	testutil "github.com/awesome-flow/flow/pkg/util/test"
)

func TestDemux_multiplex(t *testing.T) {
	tests := []struct {
		descr  string
		links  []core.Link
		expSts core.MsgStatus
	}{
		{
			"succ send",
			core_test.InitCountAndReplySet(map[string]core_test.ReplyType{
				"A": core_test.ReplyDone,
				"B": core_test.ReplyDone,
				"C": core_test.ReplyDone,
			}),
			core.MsgStatusDone,
		},
		{
			"part send",
			core_test.InitCountAndReplySet(map[string]core_test.ReplyType{
				"A": core_test.ReplyFailed,
				"B": core_test.ReplyDone,
				"C": core_test.ReplyDone,
			}),
			core.MsgStatusPartialSend,
		},
		{
			"fail send",
			core_test.InitCountAndReplySet(map[string]core_test.ReplyType{
				"A": core_test.ReplyFailed,
				"B": core_test.ReplyFailed,
				"C": core_test.ReplyFailed,
			}),
			core.MsgStatusFailed,
		},
		{
			"time out",
			core_test.InitCountAndReplySet(map[string]core_test.ReplyType{
				"A": core_test.ReplyDone,
				"B": core_test.ReplyDone,
				"C": core_test.ReplyContinue,
			}),
			core.MsgStatusTimedOut,
		},
	}

	for _, tstCase := range tests {
		t.Run(tstCase.descr, func(t *testing.T) {
			demux, err := New("demux", nil, core.NewContext())
			demux.ExecCmd(core.NewCmdStart())
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
			case s := <-msg.AckCh():
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
			links: core_test.InitCountAndReplySet(map[string]core_test.ReplyType{
				"A": core_test.ReplyDone,
				"B": core_test.ReplyDone,
				"C": core_test.ReplyDone,
			}),
			expectedCnts:   []int{1, 1, 1},
			expectedStatus: core.MsgStatusDone,
			expectedErr:    nil,
		},
		{
			name: "part send",
			links: core_test.InitCountAndReplySet(map[string]core_test.ReplyType{
				"A": core_test.ReplyDone,
				"B": core_test.ReplyDone,
				"C": core_test.ReplyFailed,
			}),
			expectedCnts:   []int{1, 1, 1},
			expectedStatus: core.MsgStatusPartialSend,
			expectedErr:    core.ErrMsgPartialSend,
		},
		{
			name: "fail send",
			links: core_test.InitCountAndReplySet(map[string]core_test.ReplyType{
				"A": core_test.ReplyFailed,
				"B": core_test.ReplyFailed,
				"C": core_test.ReplyFailed,
			}),
			expectedCnts:   []int{1, 1, 1},
			expectedStatus: core.MsgStatusFailed,
			expectedErr:    core.ErrMsgFailed,
		},
		{
			name: "time out",
			links: core_test.InitCountAndReplySet(map[string]core_test.ReplyType{
				"A": core_test.ReplyDone,
				"B": core_test.ReplyDone,
				"C": core_test.ReplyContinue,
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
			case status = <-msg.AckCh():
				if status != testCase.expectedStatus {
					t.Errorf("Unexpected status from message: %d, want: %d",
						status, testCase.expectedStatus)
				}
			case <-time.After(100 * time.Millisecond):
				t.Errorf("Timed out to receive an ack from message")
			}
			for ix, link := range testCase.links {
				linkRcvCnt := link.(*core_test.CountAndReply).RcvCnt()
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
	links := core_test.InitCountAndReplySet(map[string]core_test.ReplyType{
		"A": core_test.ReplyDone,
		"B": core_test.ReplyDone,
		"C": core_test.ReplyDone,
		"D": core_test.ReplyDone,
		"E": core_test.ReplyDone,
	})
	for i := 0; i < b.N; i++ {
		msg := core.NewMessageWithMeta(
			map[string]interface{}{
				core.MsgMetaKeySync: "true",
			},
			testutil.RandStringBytes(1024),
		)
		if err := Demultiplex(msg, DemuxMaskAll, links, 2*MsgSendTimeout); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkDemultiplexAsyncNoMeta(b *testing.B) {
	links := core_test.InitCountAndReplySet(map[string]core_test.ReplyType{
		"A": core_test.ReplyDone,
		"B": core_test.ReplyDone,
		"C": core_test.ReplyDone,
		"D": core_test.ReplyDone,
		"E": core_test.ReplyDone,
	})
	for i := 0; i < b.N; i++ {
		msg := core.NewMessage(testutil.RandStringBytes(1024))
		if err := Demultiplex(msg, DemuxMaskAll, links, 2*MsgSendTimeout); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkDemultiplexAsyncCpMeta(b *testing.B) {
	links := core_test.InitCountAndReplySet(map[string]core_test.ReplyType{
		"A": core_test.ReplyDone,
		"B": core_test.ReplyDone,
		"C": core_test.ReplyDone,
		"D": core_test.ReplyDone,
		"E": core_test.ReplyDone,
	})
	for i := 0; i < b.N; i++ {
		msg := core.NewMessageWithMeta(
			map[string]interface{}{},
			testutil.RandStringBytes(1024),
		)
		if err := Demultiplex(msg, DemuxMaskAll, links, 2*MsgSendTimeout); err != nil {
			b.Error(err)
		}
	}
}
