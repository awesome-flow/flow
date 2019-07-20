package actor

import (
	"github.com/awesome-flow/flow/pkg/cfg"
	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/types"
)

func newContextWithConfig(config map[string]interface{}) (*core.Context, error) {
	repo := cfg.NewRepository()
	for k, v := range config {
		if _, err := cfg.NewScalarConfigProvider(
			&types.KeyValue{
				Key:   types.NewKey(k),
				Value: v,
			},
			repo,
			42, // doesn't matter
		); err != nil {
			return nil, err
		}
	}

	ctx, err := core.NewContext(core.NewConfig(repo))
	if err != nil {
		return nil, err
	}

	return ctx, nil
}

func eqErr(e1, e2 error) bool {
	if e1 == nil || e2 == nil {
		return e1 == e2
	}
	return e1.Error() == e2.Error()
}

func sts2name(sts core.MsgStatus) string {
	switch sts {
	case core.MsgStatusDone:
		return "MsgStatusDone"
	case core.MsgStatusFailed:
		return "MsgStatusFailed"
	case core.MsgStatusTimedOut:
		return "MsgStatusTimedOut"
	case core.MsgStatusUnroutable:
		return "MsgStatusUnroutable"
	case core.MsgStatusThrottled:
		return "MsgStatusThrottled"
	default:
		return "Unknown"
	}
}
