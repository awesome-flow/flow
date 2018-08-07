package links

import (
	"booking/msgrelay/flow"
	"testing"
	"time"
)

type A struct {
	*flow.Connector
}

func NewA() *A {
	return &A{flow.NewConnector()}
}

type Cntr struct {
	rcvCnt int
	*flow.Connector
}

func NewCntr() *Cntr {
	return &Cntr{0, flow.NewConnector()}
}

func (cntr *Cntr) Recv(msg *flow.Message) error {
	cntr.rcvCnt++
	return msg.AckDone()
}

func TestDMX_Demultiplex(t *testing.T) {
	a1, a2 := NewA(), NewA()
	cntr := NewCntr()
	dmx, dmxErr := NewDMX("dmx", nil)
	if dmxErr != nil {
		t.Errorf("Unexpected DMX error: %s", dmxErr.Error())
	}
	if linkErr := dmx.LinkTo([]flow.Link{a1, a2}); linkErr != nil {
		t.Errorf("Failed to link dmx: %s", linkErr.Error())
	}
	dmx.ConnectTo(cntr)

	msg1 := flow.NewMessage(nil, []byte(""))
	if sendErr1 := a1.Send(msg1); sendErr1 != nil {
		t.Errorf("Unexpected a1 send error: %s", sendErr1.Error())
	}
	select {
	case s := <-msg1.GetAckCh():
		if s != flow.MsgStatusDone {
			t.Errorf("Unexpected message status: %d", s)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timed out to receive message ack")
	}
	if cntr.rcvCnt != 1 {
		t.Errorf("Unexpected received counter value: %d", cntr.rcvCnt)
	}

	msg2 := flow.NewMessage(nil, []byte(""))
	if sendErr2 := a2.Send(msg2); sendErr2 != nil {
		t.Errorf("Unexpected a2 send error: %s", sendErr2.Error())
	}
	select {
	case s := <-msg2.GetAckCh():
		if s != flow.MsgStatusDone {
			t.Errorf("Unexpected message status: %d", s)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timed out to receive message ack")
	}
	if cntr.rcvCnt != 2 {
		t.Errorf("Unexpected received counter value: %d", cntr.rcvCnt)
	}
}
