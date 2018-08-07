package flow

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestMessage_GetDst(t *testing.T) {
	type fields struct {
		Meta     MsgMeta
		Payload  []byte
		ackCh    chan MsgStatus
		attempts uint32
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:   "no sender",
			fields: fields{nil, []byte(""), nil, 0},
			want:   "undefined",
		},
		{
			name:   "yes sender",
			fields: fields{map[string]string{"sender": "expected"}, []byte(""), nil, 0},
			want:   "expected",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Message{
				Meta:     tt.fields.Meta,
				Payload:  tt.fields.Payload,
				ackCh:    tt.fields.ackCh,
				attempts: tt.fields.attempts,
			}
			if got := m.GetDst(); got != tt.want {
				t.Errorf("Message.RoutingKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMessage_IsSync(t *testing.T) {
	type fields struct {
		Meta     MsgMeta
		Payload  []byte
		ackCh    chan MsgStatus
		attempts uint32
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			"sync, digit",
			fields{map[string]string{"sync": "1"}, []byte(""), nil, 0},
			true,
		},
		{
			"sync, bool",
			fields{map[string]string{"sync": "true"}, []byte(""), nil, 0},
			true,
		},
		{
			"sync, unknown value",
			fields{map[string]string{"sync": "unknown_string"}, []byte(""), nil, 0},
			false,
		},
		{
			"no sync, non-null meta",
			fields{map[string]string{}, []byte(""), nil, 0},
			false,
		},
		{
			"no sync, null meta",
			fields{nil, []byte(""), nil, 0},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Message{
				Meta:     tt.fields.Meta,
				Payload:  tt.fields.Payload,
				ackCh:    tt.fields.ackCh,
				attempts: tt.fields.attempts,
			}
			if got := m.IsSync(); got != tt.want {
				t.Errorf("Message.IsSync() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMessage_Ack(t *testing.T) {
	NoStatus := uint8(math.MaxUint8)
	tests := []struct {
		name      string
		funcName  string
		expErr    error
		expMsgSts MsgStatus
	}{
		{"done", "AckDone", nil, MsgStatusDone},
		{"continue", "AckContinue", nil, NoStatus},
		{"invalid", "AckInvalid", ErrMsgInvalid, MsgStatusInvalid},
		{"partial", "AckPartialSend", ErrMsgPartialSend, MsgStatusPartialSend},
		{"failed", "AckFailed", ErrMsgFailed, MsgStatusFailed},
		{"timeout", "AckTimedOut", ErrMsgTimedOut, MsgStatusTimedOut},
		{"unroutable", "AckUnroutable", ErrMsgUnroutable, MsgStatusUnroutable},
		{"throttled", "AckThrottled", ErrMsgThrottled, MsgStatusThrottled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewMessage(nil, []byte(""))
			ret := reflect.ValueOf(msg).MethodByName(tt.funcName).Call([]reflect.Value{})
			err := ret[0].Interface()
			if !reflect.DeepEqual(err, tt.expErr) {
				t.Errorf("Unexpected error code: %s", err)
			}
			if tt.expMsgSts == NoStatus {
				return
			}
			select {
			case s := <-msg.ackCh:
				if s != tt.expMsgSts {
					t.Errorf("Unexpected msg status: %d", s)
				}
			case <-time.After(100 * time.Millisecond):
				t.Error("Timed out to receive a signal from msg ack chan")
			}
		})
	}
}

func TestMessage_NewMessageWithAckCh(t *testing.T) {
	ch := make(chan MsgStatus, 1)
	msg := NewMessageWithAckCh(nil, []byte(""), ch)
	msg.AckDone()
	select {
	case s := <-ch:
		if s != MsgStatusDone {
			t.Errorf("Unexpected message status: %d", s)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timed out to receive a signal from channel")
	}
}

func TestMessage_CpMessage(t *testing.T) {
	pl1 := []byte("payload1")
	meta1 := map[string]string{"k1": "v1"}
	msg1 := NewMessage(meta1, pl1)
	msg2 := CpMessage(msg1)
	msg2.Payload[len(msg2.Payload)-1] = '2'
	if !reflect.DeepEqual(msg2.Payload, []byte("payload2")) {
		t.Errorf("Unexpected payload in msg2: %s", msg2.Payload)
	}
	if !reflect.DeepEqual(msg1.Payload, pl1) {
		t.Errorf("Unexpected payload in msg1: %s", msg1.Payload)
	}
}

func TestMessage_BumpAttempts(t *testing.T) {
	msg := NewMessage(nil, []byte(""))
	if msg.attempts != 0 {
		t.Errorf("Unexpected msg attempts: %d, want: %d", msg.attempts, 0)
	}
	if err := msg.BumpAttempts(); err != nil {
		t.Errorf("Unexpected error while bumping the message attempts cntr: %s", err.Error())
	}
	if msg.attempts != 1 {
		t.Errorf("Unexpected msg attempts counter: %d, want: %d", msg.attempts, 1)
	}
}
