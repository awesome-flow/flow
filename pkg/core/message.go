package core

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
)

// MsgStatus represents the current message status.
type MsgStatus = uint8

const (
	// MsgMetaKeySync is a constant used to indicate a message is in sync mode.
	MsgMetaKeySync = "sync"
)

const (
	// MsgStatusNew represents a new message.
	MsgStatusNew MsgStatus = iota
	// MsgStatusDone represents a message that has left the pipeline.
	MsgStatusDone
	// MsgStatusPartialSend represents a partially sent message: most probably,
	// some branches of the pipeline succeeded to send it and some failed.
	MsgStatusPartialSend
	// MsgStatusInvalid represents a message recognised as invalid by the
	// pipeline components and therefore it's impossible to proceed forward.
	MsgStatusInvalid
	// MsgStatusFailed represents a message for which submission has failed.
	MsgStatusFailed
	// MsgStatusTimedOut represents a message for which one or more components
	// triggered a timeout watermark.
	MsgStatusTimedOut
	// MsgStatusUnroutable represents a message for which the submission
	// destination/branch is unknown. Most likely, a branch with the
	// corresponding name does not exist.
	MsgStatusUnroutable
	// MsgStatusThrottled represents a message which submission process was
	// cancelled due to a quota exhausting.
	MsgStatusThrottled
)

var (
	// ErrMsgPartialSend represents a partial submission error.
	ErrMsgPartialSend = fmt.Errorf("Partial message send")
	// ErrMsgInvalid represents an invalid message error.
	ErrMsgInvalid = fmt.Errorf("Invalid message format/payload")
	// ErrMsgFailed represents a failed message submission error.
	ErrMsgFailed = fmt.Errorf("Failed message send")
	// ErrMsgTimedOut represents a timeout-based message cancellation error.
	ErrMsgTimedOut = fmt.Errorf("Message send timeout")
	// ErrMsgUnroutable represents a message routing error.
	ErrMsgUnroutable = fmt.Errorf("Unroutable message")
	// ErrMsgThrottled represents a throttled message submission error.
	ErrMsgThrottled = fmt.Errorf("Message throttled")
)

// CmdCode is a type for system command passing between links.
type CmdCode int8

const (
	// CmdCodeStart represents a pipeline start command.
	CmdCodeStart CmdCode = iota
	// CmdCodeStop represents a pipeline stop command.
	CmdCodeStop
)

// Cmd is a structure that represents a single command.
type Cmd struct {
	Code    CmdCode
	payload []byte
}

// CmdPropagation represents a direction of the command propagation.
type CmdPropagation uint8

const (
	// CmdPpgtBtmUp represents a command propagation direction when it's being
	// executed bottom-up.
	CmdPpgtBtmUp = iota
	// CmdPpgtTopDwn represents a command propagation direction when it's being
	// executed top-down.
	CmdPpgtTopDwn
)

// NewCmdStart creates a new Start command.
func NewCmdStart() *Cmd {
	return &Cmd{Code: CmdCodeStart}
}

// NewCmdStop creates a new Stop command.
func NewCmdStop() *Cmd {
	return &Cmd{Code: CmdCodeStop}
}

// Message is the primary unit of information in Flow. The structure contains
// the necessary attributes to pass user message effectively through the
// pipeline.
type Message struct {
	meta     map[string]interface{}
	payload  []byte
	ackCh    chan MsgStatus
	attempts uint32
	mx       sync.Mutex
}

// NewMessage returns a new instance of Message with the payload provided.
func NewMessage(payload []byte) *Message {
	return NewMessageWithMeta(make(map[string]interface{}), payload)
}

// NewMessageWithMeta returns a new instance of Message decorated with extra
// metadata.
func NewMessageWithMeta(meta map[string]interface{}, payload []byte) *Message {
	return NewMessageWithAckCh(nil, meta, payload)
}

// NewMessageWithAckCh returns a new instance of Message decorated with extra
// metadata and using the user-defined acknowledgement channel.
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

// Meta returns Message metadata registered under the provided key.
// This method is thread-safe.
func (msg *Message) Meta(key string) (interface{}, bool) {
	msg.mx.Lock()
	defer msg.mx.Unlock()

	return msg.metaUnsafe(key)
}

func (msg *Message) metaUnsafe(key string) (interface{}, bool) {
	v, ok := msg.meta[key]
	return v, ok
}

// MetaOrDefault returns a Message metadata for the provided key or the default
// value.
func (msg *Message) MetaOrDefault(key string, def interface{}) (interface{}, bool) {
	msg.mx.Lock()
	defer msg.mx.Unlock()

	if v, ok := msg.metaUnsafe(key); ok {
		return v, true
	}

	return def, false
}

// MetaAll returns a full collection of meta attributes decorating the Message.
// This Method is thread-safe.
func (msg *Message) MetaAll() map[string]interface{} {
	msg.mx.Lock()
	defer msg.mx.Unlock()

	return msg.metaAllUnsafe()
}

func (msg *Message) metaAllUnsafe() map[string]interface{} {
	mapcp := make(map[string]interface{})
	for k, v := range msg.meta {
		mapcp[k] = v
	}

	return mapcp
}

// SetMeta registers a meta-attribute under a provided key. Meta attributes are
// life-long for messages: it might be a handy communication channel between
// links.
// This method is thread-safe.
func (msg *Message) SetMeta(key string, val interface{}) {
	msg.mx.Lock()
	defer msg.mx.Unlock()

	msg.meta[key] = val
}

// SetMetaAll registers a collection of meta-attributes in the Message meta
// storage.
// This method is thread-safe.
func (msg *Message) SetMetaAll(extMeta map[string]interface{}) {
	msg.mx.Lock()
	defer msg.mx.Unlock()

	for key, val := range extMeta {
		msg.meta[key] = val
	}
}

// UnsetMeta de-registers a previously set meta-attribute for a provided key.
// This method is thread-safe.
func (msg *Message) UnsetMeta(key string) {
	msg.mx.Lock()
	defer msg.mx.Unlock()

	delete(msg.meta, key)
}

// SetPayload is a method for explicit message payload assignment.
// This method is thread-safe.
func (msg *Message) SetPayload(payload []byte) {
	msg.mx.Lock()
	defer msg.mx.Unlock()

	msg.payload = payload
}

// Payload returns the Message byte payload value.
func (msg *Message) Payload() []byte {
	return msg.payload
}

// AckCh returns the Message ack channel instance.
func (msg *Message) AckCh() chan MsgStatus {
	return msg.ackCh
}

func (msg *Message) finalize() {
	if msg.ackCh != nil {
		close(msg.ackCh)
	}
}

// AckDone broadcasts MsgStatusDone down the Message AckCh and returns no error.
func (msg *Message) AckDone() error {
	if msg.ackCh != nil {
		msg.ackCh <- MsgStatusDone
	}
	msg.finalize()
	return nil
}

// AckContinue is a syntax sugar no-op actor used to indicate the Message
// lifecycle is not over yet and returns no error.
func (msg *Message) AckContinue() error {
	return nil
}

// AckInvalid broadcasts MsgStatusInvalid down the Message AckCh and returns
// ErrMsgInvalid.
func (msg *Message) AckInvalid() error {
	if msg.ackCh != nil {
		msg.ackCh <- MsgStatusInvalid
	}
	msg.finalize()
	return ErrMsgInvalid
}

// AckPartialSend broadcasts MsgStatusPartialSend down the Message AckCh and
// returns ErrMsgPartialSend.
func (msg *Message) AckPartialSend() error {
	if msg.ackCh != nil {
		msg.ackCh <- MsgStatusPartialSend
	}
	msg.finalize()
	return ErrMsgPartialSend
}

// AckFailed broadcasts MsgStatusFailed down the Message AckCh and returns
// ErrMsgFailed.
func (msg *Message) AckFailed() error {
	if msg.ackCh != nil {
		msg.ackCh <- MsgStatusFailed
	}
	msg.finalize()
	return ErrMsgFailed
}

// AckTimedOut broadcasts MsgStatusTimedOut down the Message AckCh and returns
// ErrMsgTimedOut.
func (msg *Message) AckTimedOut() error {
	if msg.ackCh != nil {
		msg.ackCh <- MsgStatusTimedOut
	}
	msg.finalize()
	return ErrMsgTimedOut
}

// AckUnroutable broadcasts MsgStatusUnroutable down the Message AckCh and
// returns ErrMsgUnroutable.
func (msg *Message) AckUnroutable() error {
	if msg.ackCh != nil {
		msg.ackCh <- MsgStatusUnroutable
	}
	msg.finalize()
	return ErrMsgUnroutable
}

// AckThrottled broadcasts MsgStatusThrottled down the Message AckCh and returns
// ErrMsgThrottled.
func (msg *Message) AckThrottled() error {
	if msg.ackCh != nil {
		msg.ackCh <- MsgStatusThrottled
	}
	msg.finalize()
	return ErrMsgThrottled
}

// BumpAttempts atomically increments the number of attempts the Message
// was tried to be sent.
// Returns an error if 10 attemts failed.
// This method is thread-safe.
func (msg *Message) BumpAttempts() error {
	loopBreaker := 10
	for {
		if loopBreaker < 0 {
			break
		}
		attempts := atomic.LoadUint32(&msg.attempts)
		if atomic.CompareAndSwapUint32(&msg.attempts, attempts, attempts+1) {
			return nil
		}
		loopBreaker--
	}
	return fmt.Errorf("Failed to bump message attempts")
}

// Attempts returns the number of attempts the Message was tried to be sent.
func (msg *Message) Attempts() uint32 {
	return atomic.LoadUint32(&msg.attempts)
}

// CpMessage creates a shallow copy of the Message.
// This method is thread-safe.
func CpMessage(msg *Message) *Message {
	msg.mx.Lock()
	defer msg.mx.Unlock()

	var buf bytes.Buffer
	buf.Write(msg.payload)

	return &Message{
		payload: buf.Bytes(),
		meta:    msg.metaAllUnsafe(),
		ackCh:   make(chan MsgStatus, 1),
	}
}

var msgMetaSyncValues = map[string]bool{"true": true, "1": true}

// MsgIsSync indicates if the message `sync` flag has been set on.
func MsgIsSync(msg *Message) bool {
	if sync, ok := msg.Meta(MsgMetaKeySync); sync != nil && ok {
		if _, ok = sync.(string); !ok {
			return false
		}
		if _, ok = msgMetaSyncValues[sync.(string)]; ok {
			return true
		}
	}
	return false
}
