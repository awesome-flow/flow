package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"sync"
)

var threadiness = 12

func main() {
	wg := sync.WaitGroup{}
	for i := 0; i < threadiness; i++ {
		wg.Add(1)
		go func() {
			conn, err := net.Dial("tcp", ":3101")
			if err != nil {
				wg.Done()
			}
			for {
				fmt.Fprintf(conn, "hello world\r\n")
				_, err := bufio.NewReader(conn).ReadString('\n')
				if err != nil {
					log.Printf("Failed to send data: %s", err)
				}
			}
		}()
	}
	wg.Wait()
}
