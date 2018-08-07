package receiver

import (
	"booking/bmetrics"
	"booking/msgrelay/flow"
	"fmt"
	"net"

	"github.com/facebookgo/grace/gracemulti"
	"gitlab.booking.com/go/tell"
)

const (
	MaxDatagramPayloadSize = 65536
	UdpRcvMaxThreads       = 2
)

var (
	ErrMalformedDgram = fmt.Errorf("Malformed datagram")
	ErrEmptyBody      = fmt.Errorf("Empty message body")
)

type UDP struct {
	Name   string
	Server *gracemulti.UdpServer
	*flow.Connector
}

func NewUDP(name string, params flow.Params) (flow.Link, error) {
	udpAddr, ok := params["bind_addr"]
	if !ok {
		return nil, fmt.Errorf("UDP receiver parameters are missing bind_addr")
	}
	udp := &UDP{name, nil, flow.NewConnector()}
	udp.Server = &gracemulti.UdpServer{
		Addr:    udpAddr.(string),
		Network: "udp4",
		Threads: UdpRcvMaxThreads,
		Handler: udp.updRecv,
		Data:    nil,
	}
	// TODO: make this beautiful
	var servers gracemulti.MultiServer
	servers.UDP = append(servers.UDP, udp.Server)
	go func() {
		grcErr := gracemulti.Serve(servers)
		if grcErr != nil {
			tell.Fatalf("Failed to start gracemulti servers: %s", grcErr.Error())
		}
	}()

	return udp, nil
}

func (udp *UDP) updRecv(conn *net.UDPConn, data interface{}) {

	buf := make([]byte, MaxDatagramPayloadSize)

	for {
		n, _, err := conn.ReadFrom(buf)
		bmetrics.GetOrRegisterCounter("receiver", "udp", "received").Inc(1)
		if err != nil {
			if nerr, ok := err.(net.Error); ok && (nerr.Temporary() || nerr.Timeout()) {
				bmetrics.GetOrRegisterCounter("receiver", "udp", "failed").Inc(1)
				tell.Errorf("Temporary UDP error: %s", err.Error())
				continue
			}
			tell.Errorf("UDP connection closed: %s", err.Error())
			return
		}

		msg := flow.NewMessage(nil, buf[:n])

		if sendErr := udp.Send(msg); sendErr != nil {
			tell.Errorf("UDP failed to accept message: %s", sendErr.Error())
		} else {
			bmetrics.GetOrRegisterCounter("receiver", "udp", "sent").Inc(1)
		}
	}
}
