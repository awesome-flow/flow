package actor

import (
	"fmt"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

type ReceiverHead interface{}

func ReceiverHeadFactory(params core.Params) (ReceiverHead, error) {
	//TODO
	return nil, fmt.Errorf("not implemented")
}
