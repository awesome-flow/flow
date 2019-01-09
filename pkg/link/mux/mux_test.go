package link

import (
	"testing"
	"time"

	"github.com/awesome-flow/flow/pkg/core"
)

type A struct {
	*core.Connector
}

func NewA() *A {
	return &A{core.NewConnector()}
}

type Cntr struct {
	rcvCnt int
	*core.Connector
}

func NewCntr() *Cntr {
	return &Cntr{0, core.NewConnector()}
}

func (cntr *Cntr) Recv(msg *core.Message) error {
	cntr.rcvCnt++
	return msg.AckDone()
}

func TestMux_Demultiplex(t *testing.T) {
	a1, a2 := NewA(), NewA()
	cntr := NewCntr()
	mux, muxErr := New("mux", nil, core.NewContext())
	if muxErr != nil {
		t.Errorf("Unexpected Mux error: %s", muxErr.Error())
	}
	if linkErr := mux.LinkTo([]core.Link{a1, a2}); linkErr != nil {
		t.Errorf("Failed to link mux: %s", linkErr.Error())
	}
	mux.ConnectTo(cntr)

	msg1 := core.NewMessage([]byte{})
	if sendErr1 := a1.Send(msg1); sendErr1 != nil {
		t.Errorf("Unexpected a1 send error: %s", sendErr1.Error())
	}
	select {
	case s := <-msg1.GetAckCh():
		if s != core.MsgStatusDone {
			t.Errorf("Unexpected message status: %d", s)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timed out to receive message ack")
	}
	if cntr.rcvCnt != 1 {
		t.Errorf("Unexpected received counter value: %d", cntr.rcvCnt)
	}

	msg2 := core.NewMessage([]byte{})
	if sendErr2 := a2.Send(msg2); sendErr2 != nil {
		t.Errorf("Unexpected a2 send error: %s", sendErr2.Error())
	}
	select {
	case s := <-msg2.GetAckCh():
		if s != core.MsgStatusDone {
			t.Errorf("Unexpected message status: %d", s)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timed out to receive message ack")
	}
	if cntr.rcvCnt != 2 {
		t.Errorf("Unexpected received counter value: %d", cntr.rcvCnt)
	}
}
