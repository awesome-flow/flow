package tcp_server

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type stts struct {
	cnt   uint64
	bytes uint64
}

func main() {

	flag.Parse()

	port, ok := os.LookupEnv("TCP_SERVER_PORT")
	if !ok {
		panic("TCP_SERVER_PORT env variable must be set")
	}
	bindAddr := fmt.Sprintf("0.0.0.0:%s", port)

	fmt.Printf("Starting a TCP server on %s\n", bindAddr)

	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		panic(fmt.Sprintf("Unable to bind on addr %s: %s", bindAddr, err.Error()))
	}
	defer listener.Close()
	acc := &sync.Map{}
	go reportEpoch(acc)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Warning: Unable to accept connection: %s\n", err.Error())
		}
		go handleRequest(conn, acc)
	}
}

func reportEpoch(acc *sync.Map) {
	epoch := time.Now().Unix()
	var curEpoch int64
	for {
		curEpoch = time.Now().Unix()
		if curEpoch != epoch {
			go doReport(acc, epoch)
			epoch = curEpoch
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func doReport(acc *sync.Map, epoch int64) {
	if bucket, ok := acc.Load(epoch); ok {
		fmt.Printf("Epoch %d received %d messages, %d bytes\n",
			epoch, bucket.(*stts).cnt, bucket.(*stts).bytes)
	} else {
		fmt.Printf("Epoch %d is empty\n", epoch)
	}
}

func handleRequest(conn net.Conn, acc *sync.Map) {
	buf := make([]byte, 65536)
	fmt.Printf("%s connected\n", conn.RemoteAddr())
	for {
		n, err := conn.Read(buf)
		if err != nil {
			switch err {
			case io.EOF:
				fmt.Printf("%s disconnected\n", conn.RemoteAddr())
			default:
				fmt.Printf("Warning: Failed to read from connection: %s\n", err.Error())
			}
			break
		}
		if n == 0 {
			break
		}
		epoch := time.Now().Unix()
		bucket, _ := acc.LoadOrStore(
			epoch,
			&stts{cnt: 0, bytes: 0})
		v := bucket.(*stts)
		atomic.AddUint64(&v.cnt, 1)
		atomic.AddUint64(&v.bytes, uint64(n))
	}
	conn.Close()
}
