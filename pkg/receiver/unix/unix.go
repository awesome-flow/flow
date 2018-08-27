package receiver

import (
	"fmt"
	"io"
	"net"

	"github.com/facebookgo/grace/gracenet"
	log "github.com/sirupsen/logrus"
	"github.com/whiteboxio/flow/pkg/core"
	"github.com/whiteboxio/flow/pkg/metrics"
)

const (
	MaxUnixPayloadSize = 65536
)

var (
	ErrMalformedUnixgram = fmt.Errorf("Malformed unixgram")
	ErrEmptyBody         = fmt.Errorf("Empty message body")
)

type Unix struct {
	Name     string
	listener net.Listener
	*core.Connector
}

func NewUnix(name string, params core.Params) (core.Link, error) {
	path, ok := params["path"]
	if !ok {
		path = "/tmp/flow.sock"
	}

	net := &gracenet.Net{}
	lstnr, err := net.Listen("unix", path.(string))
	if err != nil {
		return nil, err
	}
	ux := &Unix{name, lstnr, core.NewConnector()}
	go func() {
		for {
			fd, err := lstnr.Accept()
			if err != nil {
				log.Errorf("Unix listener failed to call accept: %s", err.Error())
				continue
			}
			go unixRecv(ux, fd)
		}
	}()
	return ux, nil
}

func (ux *Unix) ExecCmd(cmd *core.Cmd) error {
	switch cmd.Code {
	case core.CmdCodeStop:
		if err := ux.listener.Close(); err != nil {
			log.Warnf("Failed to close unix socket properly: %s", err.Error())
		}
	}
	return nil
}

func unixRecv(ux *Unix, conn net.Conn) {
	buf := make([]byte, MaxUnixPayloadSize)
	for {
		n, err := conn.Read(buf)
		metrics.GetCounter("receiver.unix.received").Inc(1)

		if err == io.EOF {
			log.Infof("Met EOF")
			return
		}

		if err != nil {
			log.Warnf("Unix conn Read failed: %s %+v", err.Error(), err)
			return
		}
		if n == 0 {
			return
		}
		msg := core.NewMessage(buf[:n])

		if sendErr := ux.Send(msg); sendErr != nil {
			log.Errorf("Unix socket failed to send message: %s", sendErr.Error())
		} else {
			metrics.GetCounter("receiver.unix.sent").Inc(1)
		}
	}
}
