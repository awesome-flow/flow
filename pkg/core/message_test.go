package core

import (
	"math"
	"reflect"
	"testing"
	"time"
)

type resFound struct {
	key   string
	res   interface{}
	found bool
}

func TestMessageGetMeta(t *testing.T) {
	tests := []struct {
		name     string
		meta     map[string]interface{}
		expected []resFound
	}{
		{
			name: "plain meta",
			meta: map[string]interface{}{
				"foo": "bar",
				"bar": "baz",
			},
			expected: []resFound{
				{"foo", "bar", true},
				{"bar", "baz", true},
				{"", "", false},
				{"foobar", "", false},
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			msg := NewMessageWithMeta(testCase.meta, []byte{})
			for _, exp := range testCase.expected {
				v, ok := msg.GetMeta(exp.key)
				if exp.found != ok {
					t.Errorf("Found flag does not match: %t, want: %t", ok, exp.found)
				}
				if exp.found {
					if v.(string) != exp.res.(string) {
						t.Errorf("Unexpected val for key %s: %s, want: %s",
							exp.key, v, exp.res)
					}
				}
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
			msg := NewMessage([]byte(""))
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
	msg := NewMessageWithAckCh(ch, nil, []byte(""))
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
	meta1 := map[string]interface{}{"k1": "v1"}
	msg1 := NewMessageWithMeta(meta1, pl1)
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
	msg := NewMessage([]byte(""))
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
