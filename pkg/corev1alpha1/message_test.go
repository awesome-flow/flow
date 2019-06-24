package corev1alpha1

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	testutil "github.com/awesome-flow/flow/pkg/util/test"
)

func TestNewMessage(t *testing.T) {
	body := testutil.RandBytes(1024)
	msg := NewMessage(body)
	if !reflect.DeepEqual(msg.body, body) {
		t.Fatalf("unexpected message body: got: %s, want: %s", msg.body, body)
	}
	if msg.status != MsgStatusNew {
		t.Fatalf("unexpected message status: got %d, want: %d", msg.status, MsgStatusNew)
	}
}

func TestNewMessageCopyBody(t *testing.T) {
	body := testutil.RandBytes(1024)
	msg := NewMessage(body)
	// RandBytes guarantees to place a non-zero byte at any pos
	body[rand.Intn(len(body))] = '\x00'
	if reflect.DeepEqual(msg.body, body) {
		t.Fatalf("detected a side affect of the original body modification")
	}
}

func TestAwait(t *testing.T) {
	done := make(chan struct{})
	msg := NewMessage(testutil.RandBytes(1024))
	go func() {
		msg.Await()
		close(done)
	}()
	msg.Complete(MsgStatusDone)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("timed out to await")
	}
}

func TestAwaitChan(t *testing.T) {
	msg := NewMessage(testutil.RandBytes(1024))
	go func() {
		msg.Complete(MsgStatusDone)
	}()
	select {
	case <-msg.AwaitChan():
	case <-time.After(time.Second):
		t.Fatalf("timed out to await chan")
	}
}
