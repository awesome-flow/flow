package corev1alpha1

type Params map[interface{}]interface{}

type Constructor func(name string, ctx *Context, params Params) (Actor, error)
