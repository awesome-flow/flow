package main

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/awesome-flow/flow/pkg/core"
	replicator "github.com/awesome-flow/flow/pkg/link/replicator"
	tcp_rcv "github.com/awesome-flow/flow/pkg/receiver/tcp"
)

var cntr uint64

func main() {
	log.Printf("Starting a new TCP listener")
	tcprcv, err := tcp_rcv.New("tcp_rcv", core.Params{
		"bind_addr": ":3101",
		"mode":      "talkative",
		"backend":   "std",
	}, core.NewContext())
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize TCP receiver: %s", err))
	}

	repl, err := replicator.New("replicator", core.Params{"replicas": 3}, core.NewContext())
	if err != nil {
		panic(err.Error())
	}
	links := make([]core.Link, 0, 10)
	for i := 0; i < 10; i++ {
		links = append(links, &sink{core.NewConnector()})
	}
	tcprcv.ConnectTo(repl)
	repl.LinkTo(links)

	cntr = 0

	go reportCnt()
	if err := repl.SetUp(); err != nil {
		panic(err.Error())
	}
	if err := tcprcv.SetUp(); err != nil {
		panic(err.Error())
	}

	done := make(chan struct{})

	<-done
}

func reportCnt() {
	v := atomic.SwapUint64(&cntr, 0)
	log.Printf("Counter: %d", v)
	time.Sleep(time.Second)
	go reportCnt()
}

type recv struct {
	*core.Connector
}

type sink struct {
	*core.Connector
}

func (l *sink) Recv(msg *core.Message) error {
	atomic.AddUint64(&cntr, 1)
	return msg.AckDone()
}
