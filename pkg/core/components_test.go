package core

import (
	//"sync"
	"testing"
	"time"
	//"github.com/whiteboxio/flow/pkg/util/test"
)

type A struct {
	*Connector
}

func NewA() *A {
	a := &A{
		NewConnector(),
	}
	go func() {
		for {
			time.Sleep(50 * time.Millisecond)
			a.Send(&Message{})
		}
	}()
	return a
}

func (a *A) Recv(*Message) error {
	panic("A is not supposed to receive messages")
}

type B struct {
	rcvCnt int
	*Connector
}

func NewB() *B {
	return &B{0, NewConnector()}
}

func (b *B) Recv(msg *Message) error {
	b.rcvCnt++
	return b.Send(msg)
}

type C struct {
	rcvCh chan bool
}

func NewC() *C {
	return &C{make(chan bool, 1)}
}

func (c *C) Recv(msg *Message) error {
	c.rcvCh <- true
	return nil
}

func (c *C) Send(msg *Message) error {
	panic("C is not supposed to send any messages")
}

func (c *C) ConnectTo(Link) error {
	panic("C is not supposed to connect to any links")
}
func (c *C) LinkTo([]Link) error {
	panic("C is not supposed to link to any links")
}
func (c *C) RouteTo(map[string]Link) error {
	panic("C is not supposed to route to any links")
}
func (c *C) ExecCmd(cmd *Cmd) error {
	return nil
}

func (c *C) String() string { return "a C instance" }

func (c *C) GetContext() *Context { return nil }

func Test2ConnectedLinks(t *testing.T) {
	a := NewA()
	c := NewC()
	a.ConnectTo(c)
	select {
	case <-c.rcvCh:
		t.Log("Received a message in C")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Failed to receive the message in C")
	}
}

func Test3ConnectedLinks(t *testing.T) {
	a := NewA()
	b := NewB()
	c := NewC()
	a.ConnectTo(b)
	b.ConnectTo(c)
	select {
	case <-c.rcvCh:
		t.Log("Received a message in C")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Failed to receive the message in C")
	}
	if b.rcvCnt != 1 {
		t.Fatalf("Unexpected rcv counter in B: %d", b.rcvCnt)
	}
}

// ===== Benchmarks =====

// type consumer struct {
// 	cnt uint64
// 	*Connector
// }
//
// func newConsumer(ctx *Context) *consumer {
// 	return &consumer{0, NewConnectorWithContext(ctx)}
// }
//
// func (c *consumer) Recv(msg *Message) error {
// 	c.cnt++
// 	return nil
// }
//
// func BenchmarkSendRecvUno(b *testing.B) {
// 	doBenchmarkSendRecv(b, 1, 1)
// }
//
// func BenchmarkSendRecvDuo(b *testing.B) {
// 	doBenchmarkSendRecv(b, 2, 1)
// }
//
// func BenchmarkSendRecvQuadro(b *testing.B) {
// 	doBenchmarkSendRecv(b, 4, 1)
// }
//
// func BenchmarkSendRecvOcta(b *testing.B) {
// 	doBenchmarkSendRecv(b, 8, 1)
// }
//
// func doBenchmarkSendRecv(b *testing.B, threadiness int, bufSize int) {
//
// 	contexts := make([]*Context, threadiness+1) // +1 for the receiver
// 	for i := 0; i < threadiness+1; i++ {
// 		msgChannels := make([]chan *Message, threadiness)
// 		for i := 0; i < threadiness; i++ {
// 			msgChannels[i] = make(chan *Message, bufSize)
// 		}
// 		contexts[i] = NewContextUnsafe(msgChannels, make(chan *Cmd), make(chan *Cmd), &sync.Map{})
// 	}
// 	receiver := NewConnectorWithContext(contexts[threadiness]) // the last one
// 	consumers := make([]*consumer, threadiness)
// 	for i := 0; i < threadiness; i++ {
// 		consumers[i] = newConsumer(contexts[i])
// 		receiver.ConnectTo(consumers[i])
// 	}
// 	for i := 0; i < b.N; i++ {
// 		msg := NewMessage(test.RandStringBytes(1024))
// 		if err := receiver.Recv(msg); err != nil {
// 			panic(err)
// 		}
// 	}
// }
