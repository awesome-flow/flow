package flow

import (
	"fmt"
	"sync/atomic"
)

type MsgStatus = uint8

const (
	MsgStatusNew MsgStatus = iota
	MsgStatusDone
	MsgStatusPartialSend
	MsgStatusInvalid
	MsgStatusFailed
	MsgStatusTimedOut
	MsgStatusUnroutable
	MsgStatusThrottled
)

var (
	ErrMsgPartialSend = fmt.Errorf("Message was sent partialy")
	ErrMsgInvalid     = fmt.Errorf("Invalid message format/payload")
	ErrMsgFailed      = fmt.Errorf("Complete was not sent")
	ErrMsgTimedOut    = fmt.Errorf("Message send timeout")
	ErrMsgUnroutable  = fmt.Errorf("Unroutable message")
	ErrMsgThrottled   = fmt.Errorf("Message throttled")
)

type CmdCode int8

const (
	CmdCodeStart CmdCode = iota
	CmdCodeStop
)

type MsgMeta map[string]string

func NewMsgMeta() MsgMeta {
	return make(map[string]string)
}

type Message struct {
	Meta     MsgMeta
	Payload  []byte
	ackCh    chan MsgStatus
	attempts uint32
}

type Cmd struct {
	Code    CmdCode
	Payload []byte
}

type CmdPropagation uint8

const (
	CmdPpgtBtmUp = iota
	CmdPpgtTopDwn
)

func NewMessage(meta MsgMeta, payload []byte) *Message {
	return NewMessageWithAckCh(meta, payload, nil)
}

func NewMessageWithAckCh(meta MsgMeta, payload []byte, ackCh chan MsgStatus) *Message {
	if ackCh == nil {
		ackCh = make(chan MsgStatus, 1)
	}
	return &Message{
		Meta:     meta,
		Payload:  payload,
		ackCh:    ackCh,
		attempts: 0,
	}
}

func (m *Message) GetDst() string {
	if m.Meta != nil {
		if s, ok := m.Meta["sender"]; ok { // TODO: sender is a suboptimal name
			return s
		}
	}
	return "undefined"
}

func (m *Message) GetAckCh() chan MsgStatus {
	return m.ackCh
}

func (m *Message) IsSync() bool {
	if v, ok := m.Meta["sync"]; ok {
		sync := v
		if sync == "true" || sync == "1" {
			return true
		}
	}
	return false
}

func (m *Message) finalize() {
	if m.ackCh != nil {
		close(m.ackCh)
	}
}

func (m *Message) AckDone() error {
	if m.ackCh != nil {
		m.ackCh <- MsgStatusDone
	}
	m.finalize()
	return nil
}

func (m *Message) AckContinue() error {
	return nil
}

func (m *Message) AckInvalid() error {
	if m.ackCh != nil {
		m.ackCh <- MsgStatusInvalid
	}
	m.finalize()
	return ErrMsgInvalid
}

func (m *Message) AckPartialSend() error {
	if m.ackCh != nil {
		m.ackCh <- MsgStatusPartialSend
	}
	m.finalize()
	return ErrMsgPartialSend
}

func (m *Message) AckFailed() error {
	if m.ackCh != nil {
		m.ackCh <- MsgStatusFailed
	}
	m.finalize()
	return ErrMsgFailed
}

func (m *Message) AckTimedOut() error {
	if m.ackCh != nil {
		m.ackCh <- MsgStatusTimedOut
	}
	m.finalize()
	return ErrMsgTimedOut
}

func (m *Message) AckUnroutable() error {
	if m.ackCh != nil {
		m.ackCh <- MsgStatusUnroutable
	}
	m.finalize()
	return ErrMsgUnroutable
}

func (m *Message) AckThrottled() error {
	if m.ackCh != nil {
		m.ackCh <- MsgStatusThrottled
	}
	m.finalize()
	return ErrMsgThrottled
}

func (m *Message) BumpAttempts() error {
	loopBreaker := 10
	for {
		if loopBreaker < 0 {
			break
		}
		attempts := atomic.LoadUint32(&m.attempts)
		if atomic.CompareAndSwapUint32(&m.attempts, attempts, attempts+1) {
			return nil
		}
		loopBreaker--
	}
	return fmt.Errorf("Failed to bump message attempts")
}

func (m *Message) GetAttempts() uint32 {
	return atomic.LoadUint32(&m.attempts)
}

func CpMessage(m *Message) *Message {
	return &Message{
		Meta:    m.Meta,
		Payload: m.Payload,
		ackCh:   make(chan MsgStatus, 1),
	}
}
