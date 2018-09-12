package link

import (
	"testing"
	"time"

	"github.com/whiteboxio/flow/pkg/core"
)

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
