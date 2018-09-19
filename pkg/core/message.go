package core

import (
	"fmt"
	"sync"
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

type MsgMeta sync.Map

type Message struct {
	meta     *sync.Map
	Payload  []byte
	ackCh    chan MsgStatus
	attempts uint32
	mx       sync.Mutex
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

func NewMessage(payload []byte) *Message {
	return NewMessageWithMeta(nil, payload)
}

func NewMessageWithMeta(meta map[string]interface{}, payload []byte) *Message {
	return NewMessageWithAckCh(nil, meta, payload)
}

func NewMessageWithAckCh(ackCh chan MsgStatus, meta map[string]interface{}, payload []byte) *Message {
	msg := &Message{
		Payload: payload,
	}
	if meta != nil {
		syncMeta := &sync.Map{}
		for k, v := range meta {
			syncMeta.Store(k, v)
		}
		msg.meta = syncMeta
	}
	if ackCh == nil {
		ackCh = make(chan MsgStatus, 1)
	}
	msg.ackCh = ackCh
	return msg
}

func (m *Message) GetMeta(key string) (interface{}, bool) {
	if m.meta == nil {
		return nil, false
	}
	return m.meta.Load(key)
}

func (m *Message) GetMetaOrDef(key string, def interface{}) (interface{}, bool) {
	if v, ok := m.GetMeta(key); ok {
		return v, true
	}
	return def, false
}

func (m *Message) GetMetaAll() map[string]interface{} {
	m.mx.Lock()
	defer m.mx.Unlock()
	res := make(map[string]interface{})
	if m.meta != nil {
		m.meta.Range(func(key, val interface{}) bool {
			res[key.(string)] = val
			return true
		})
	}
	return res
}

func (m *Message) SetMeta(key string, val interface{}) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.meta == nil {
		m.meta = &sync.Map{}
	}
	m.meta.Store(key, val)
}

func (m *Message) SetMetaAll(extMeta map[string]interface{}) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.meta == nil {
		m.meta = &sync.Map{}
	}
	for k, v := range extMeta {
		m.meta.Store(k, v)
	}
}

func (m *Message) UnsetMeta(key string) (interface{}, bool) {
	m.mx.Lock()
	defer m.mx.Unlock()
	if m.meta != nil {
		v, ok := m.meta.Load(key)
		m.meta.Delete(key)
		return v, ok
	}
	return nil, false
}

func (m *Message) UnsetMetaAll() map[string]interface{} {
	m.mx.Lock()
	defer m.mx.Unlock()
	res := make(map[string]interface{})
	if m.meta != nil {
		m.meta.Range(func(key, val interface{}) bool {
			res[key.(string)] = val
			return true
		})
		m.meta = nil
	}
	return res
}

func (m *Message) GetAckCh() chan MsgStatus {
	return m.ackCh
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
		meta:    m.meta,
		Payload: m.Payload,
		ackCh:   make(chan MsgStatus, 1),
	}
}
