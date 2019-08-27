package actor

import (
	"fmt"
	"strings"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

func ReceiverFactory(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	b, ok := params["bind"]
	if !ok {
		return nil, fmt.Errorf("receiver %q is missing `bind` config", name)
	}
	bind := b.(string)
	var builder core.Builder
	switch {
	case strings.HasPrefix(bind, "tcp://"):
		bind = bind[6:]
		builder = NewReceiverTCP
	case strings.HasPrefix(bind, "udp://"):
		bind = bind[6:]
		builder = NewReceiverUDP
	case strings.HasPrefix(bind, "unix://"):
		bind = bind[7:]
		builder = NewReceiverUnix
	case strings.HasPrefix(bind, "http://"):
		bind = bind[7:]
		builder = NewReceiverHTTP
	default:
		return nil, fmt.Errorf("receiver %q has unrecognised `bind` protocol: %q", name, bind)
	}

	params["bind"] = bind

	return builder(name, ctx, params)
}
