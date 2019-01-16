package core

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
)

type MsgStatus = uint8

const (
	MsgMetaKeySync = "sync"
)

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
	ErrMsgPartialSend = fmt.Errorf("Partial message send")
	ErrMsgInvalid     = fmt.Errorf("Invalid message format/payload")
	ErrMsgFailed      = fmt.Errorf("Failed message send")
	ErrMsgTimedOut    = fmt.Errorf("Message send timeout")
	ErrMsgUnroutable  = fmt.Errorf("Unroutable message")
	ErrMsgThrottled   = fmt.Errorf("Message throttled")
)

type CmdCode int8

const (
	CmdCodeStart CmdCode = iota
	CmdCodeStop
)

type Cmd struct {
	Code    CmdCode
	payload []byte
}

type CmdPropagation uint8

const (
	CmdPpgtBtmUp = iota
	CmdPpgtTopDwn
)

type Message struct {
	meta     map[string]interface{}
	payload  []byte
	ackCh    chan MsgStatus
	attempts uint32
	mx       sync.Mutex
}

func NewMessage(payload []byte) *Message {
	return NewMessageWithMeta(make(map[string]interface{}), payload)
}

func NewMessageWithMeta(meta map[string]interface{}, payload []byte) *Message {
	return NewMessageWithAckCh(nil, meta, payload)
}

func NewMessageWithAckCh(ackCh chan MsgStatus, meta map[string]interface{}, payload []byte) *Message {
	if ackCh == nil {
		ackCh = make(chan MsgStatus, 1)
	}
	msg := &Message{
		payload: payload,
		meta:    meta,
		ackCh:   ackCh,
		mx:      sync.Mutex{},
	}

	return msg
}

func (msg *Message) GetMeta(key string) (interface{}, bool) {
	msg.mx.Lock()
	defer msg.mx.Unlock()

	return msg.getMetaUnsafe(key)

}

func (msg *Message) getMetaUnsafe(key string) (interface{}, bool) {
	v, ok := msg.meta[key]
	return v, ok
}

func (msg *Message) GetMetaOrDef(key string, def interface{}) (interface{}, bool) {
	msg.mx.Lock()
	defer msg.mx.Unlock()

	if v, ok := msg.getMetaUnsafe(key); ok {
		return v, true
	}

	return def, false
}

func (msg *Message) GetMetaAll() map[string]interface{} {
	msg.mx.Lock()
	defer msg.mx.Unlock()

	return msg.getMetaAllUnsafe()
}

func (msg *Message) getMetaAllUnsafe() map[string]interface{} {
	mapcp := make(map[string]interface{})
	for k, v := range msg.meta {
		mapcp[k] = v
	}
	return mapcp
}

func (msg *Message) SetMeta(key string, val interface{}) {
	msg.mx.Lock()
	defer msg.mx.Unlock()

	msg.meta[key] = val
}

func (msg *Message) SetMetaAll(extMeta map[string]interface{}) {
	msg.mx.Lock()
	defer msg.mx.Unlock()

	for key, val := range extMeta {
		msg.meta[key] = val
	}
}

func (msg *Message) UnsetMeta(key string) {
	msg.mx.Lock()
	defer msg.mx.Unlock()

	delete(msg.meta, key)
}

func (msg *Message) SetPayload(payload []byte) {
	msg.mx.Lock()
	defer msg.mx.Unlock()

	msg.payload = payload
}

func (msg *Message) Payload() []byte {
	return msg.payload
}

func (msg *Message) GetAckCh() chan MsgStatus {
	return msg.ackCh
}

func (msg *Message) finalize() {
	if msg.ackCh != nil {
		close(msg.ackCh)
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

func (m *Message) Attempts() uint32 {
	return atomic.LoadUint32(&m.attempts)
}

func CpMessage(msg *Message) *Message {
	msg.mx.Lock()
	defer msg.mx.Unlock()

	var buf bytes.Buffer
	buf.Write(msg.payload)

	return &Message{
		payload: buf.Bytes(),
		meta:    msg.getMetaAllUnsafe(),
		ackCh:   make(chan MsgStatus, 1),
	}
}

var msgMetaSyncValues = map[string]bool{"true": true, "1": true}

func MsgIsSync(msg *Message) bool {
	if sync, ok := msg.GetMeta(MsgMetaKeySync); sync != nil && ok {
		if _, ok = sync.(string); !ok {
			return false
		}
		if _, ok = msgMetaSyncValues[sync.(string)]; ok {
			return true
		}
	}
	return false
}
