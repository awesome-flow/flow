package links

import (
	"booking/msgrelay/flow"
	"testing"
	"time"
)

type A struct {
	rcvCnt int
	*flow.Connector
}

func NewA() *A { return &A{0, flow.NewConnector()} }

// This function always marks messages as done
func (a *A) Recv(msg *flow.Message) error {
	a.rcvCnt++
	return msg.AckDone()
}

type B struct {
	rcvCnt int
	*flow.Connector
}

func NewB() *B { return &B{0, flow.NewConnector()} }

// This function always marks messages as failed
func (b *B) Recv(msg *flow.Message) error {
	b.rcvCnt++
	return msg.AckFailed()
}

type C struct {
	rcvCnt int
	*flow.Connector
}

func NewC() *C { return &C{0, flow.NewConnector()} }

// This function never acks the message, so it should time out
func (c *C) Recv(msg *flow.Message) error {
	c.rcvCnt++
	return nil
}

func TestMPX_multiplex(t *testing.T) {
	tests := []struct {
		descr  string
		links  []flow.Link
		expSts flow.MsgStatus
	}{
		{"succ send", []flow.Link{NewA(), NewA(), NewA()}, flow.MsgStatusDone},
		{"part send", []flow.Link{NewB(), NewA(), NewA()}, flow.MsgStatusPartialSend},
		{"fail send", []flow.Link{NewB(), NewB(), NewB()}, flow.MsgStatusFailed},
		{"time out", []flow.Link{NewA(), NewA(), NewC()}, flow.MsgStatusPartialSend},
	}

	for _, tstCase := range tests {
		t.Run(tstCase.descr, func(t *testing.T) {
			mpx, err := NewMPX("mpx", nil)
			if err != nil {
				t.Errorf("Unexpected mxp init error: %s", err.Error())
			}
			linkErr := mpx.LinkTo(tstCase.links)
			if linkErr != nil {
				t.Errorf("Failed to connect links to mpx: %s", linkErr.Error())
			}
			msg := flow.NewMessage(nil, []byte(""))
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
