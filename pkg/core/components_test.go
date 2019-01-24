package core

import (
	"sync"
	"testing"
	"time"
	//"github.com/awesome-flow/flow/pkg/util/test"
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
	mx     *sync.Mutex
	*Connector
}

func NewB() *B {
	return &B{0, &sync.Mutex{}, NewConnector()}
}

func (b *B) Recv(msg *Message) error {
	b.mx.Lock()
	defer b.mx.Unlock()
	b.rcvCnt++
	return b.Send(msg)
}

func (b *B) RcvCnt() int {
	b.mx.Lock()
	defer b.mx.Unlock()
	return b.rcvCnt
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

func (c *C) SetUp() error    { return nil }
func (c *C) TearDown() error { return nil }

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
	if b.RcvCnt() != 1 {
		t.Fatalf("Unexpected rcv counter in B: %d", b.rcvCnt)
	}
}

type link struct {
	setup    int
	teardown int
	*Connector
}

func newLink() *link {
	l := &link{0, 0, NewConnector()}
	l.OnSetUp(l.SetUp)
	l.OnTearDown(l.TearDown)
	return l
}

func (l *link) SetUp() error {
	l.setup++
	return nil
}

func (l *link) TearDown() error {
	l.teardown++
	return nil
}

// Start should call SetUp only once
func TestStart(t *testing.T) {
	l := newLink()
	if l.setup != 0 {
		t.Fatalf("unexpected setup counter: %d, want: 0", l.setup)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			if err := l.Start(); err != nil {
				t.Fatalf("Failed to start link: %s", err)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	if l.setup != 1 {
		t.Fatalf("unexpected setup counter: %d, want: 1", l.setup)
	}
}
