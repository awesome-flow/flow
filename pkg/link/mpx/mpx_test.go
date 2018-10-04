package link

import (
	"reflect"
	"testing"
	"time"

	"github.com/whiteboxio/flow/pkg/core"
)

type CntRcvd interface {
	GetRcvCnt() int
}

type A struct {
	rcvCnt int
	*core.Connector
}

func NewA() *A { return &A{0, core.NewConnector()} }

// This function always marks messages as done
func (a *A) Recv(msg *core.Message) error {
	a.rcvCnt++
	return msg.AckDone()
}

func (a *A) GetRcvCnt() int { return a.rcvCnt }

type B struct {
	rcvCnt int
	*core.Connector
}

func NewB() *B { return &B{0, core.NewConnector()} }

// This function always marks messages as failed
func (b *B) Recv(msg *core.Message) error {
	b.rcvCnt++
	return msg.AckFailed()
}

func (b *B) GetRcvCnt() int { return b.rcvCnt }

type C struct {
	rcvCnt int
	*core.Connector
}

func NewC() *C { return &C{0, core.NewConnector()} }

// This function never acks the message, so it should time out
func (c *C) Recv(msg *core.Message) error {
	c.rcvCnt++
	return nil
}

func (c *C) GetRcvCnt() int { return c.rcvCnt }

func TestMPX_multiplex(t *testing.T) {
	tests := []struct {
		descr  string
		links  []core.Link
		expSts core.MsgStatus
	}{
		{"succ send", []core.Link{NewA(), NewA(), NewA()}, core.MsgStatusDone},
		{"part send", []core.Link{NewB(), NewA(), NewA()}, core.MsgStatusPartialSend},
		{"fail send", []core.Link{NewB(), NewB(), NewB()}, core.MsgStatusFailed},
		{"time out", []core.Link{NewA(), NewA(), NewC()}, core.MsgStatusPartialSend},
	}

	for _, tstCase := range tests {
		t.Run(tstCase.descr, func(t *testing.T) {
			mpx, err := New("mpx", nil)
			if err != nil {
				t.Errorf("Unexpected mxp init error: %s", err.Error())
			}
			linkErr := mpx.LinkTo(tstCase.links)
			if linkErr != nil {
				t.Errorf("Failed to connect links to mpx: %s", linkErr.Error())
			}
			msg := core.NewMessage([]byte(""))
			if rcvErr := mpx.Recv(msg); rcvErr != nil {
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

func Test_Multiplex(t *testing.T) {
	tests := []struct {
		name           string
		links          []core.Link
		expectedCnts   []int
		expectedStatus core.MsgStatus
		expectedErr    error
	}{
		{
			name:           "succ send",
			links:          []core.Link{NewA(), NewA(), NewA()},
			expectedCnts:   []int{1, 1, 1},
			expectedStatus: core.MsgStatusDone,
			expectedErr:    nil,
		},
		{
			name:           "part send",
			links:          []core.Link{NewA(), NewA(), NewB()},
			expectedCnts:   []int{1, 1, 1},
			expectedStatus: core.MsgStatusPartialSend,
			expectedErr:    core.ErrMsgPartialSend,
		},
		{
			name:           "fail send",
			links:          []core.Link{NewB(), NewB(), NewB()},
			expectedCnts:   []int{1, 1, 1},
			expectedStatus: core.MsgStatusFailed,
			expectedErr:    core.ErrMsgFailed,
		},
		{
			name:           "time out",
			links:          []core.Link{NewA(), NewA(), NewC()},
			expectedCnts:   []int{1, 1, 1},
			expectedStatus: core.MsgStatusTimedOut,
			expectedErr:    core.ErrMsgTimedOut,
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			msg := core.NewMessage([]byte("hello world"))
			err := Multiplex(msg, testCase.links, 50*time.Millisecond)
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
				linkRcvCnt := link.(CntRcvd).GetRcvCnt()
				if linkRcvCnt != testCase.expectedCnts[ix] {
					t.Errorf("Unexpected rcv count: %d, want: %d",
						linkRcvCnt, testCase.expectedCnts[ix])
				}
			}
		})
	}
}
