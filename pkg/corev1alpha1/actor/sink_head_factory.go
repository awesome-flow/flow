package actor

import (
	"fmt"
	"net"
	"strings"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

type SinkHead interface {
	core.Runner
	Write([]byte) (int, error, bool)
	Connect() error
}

func SinkHeadFactory(params core.Params) (SinkHead, error) {
	b, ok := params["bind"]
	if !ok {
		return nil, fmt.Errorf("missing `bind` config")
	}
	bind := b.(string)
	if strings.HasPrefix(bind, "tcp://") {
		tcpaddr, err := net.ResolveTCPAddr("tcp", bind[6:])
		if err != nil {
			return nil, err
		}
		return NewSinkHeadTCP(tcpaddr)
	} else if strings.HasPrefix(bind, "udp://") {
		udpaddr, err := net.ResolveUDPAddr("udp", bind[6:])
		if err != nil {
			return nil, err
		}
		return NewSinkHeadUDP(udpaddr)
	} else if strings.HasPrefix(bind, "unix://") {
		unixaddr, err := net.ResolveUnixAddr("unix", bind[7:])
		if err != nil {
			return nil, err
		}
		return NewSinkHeadUnix(unixaddr)
	}

	return nil, fmt.Errorf("unrecognised address format: %q", bind)
}
