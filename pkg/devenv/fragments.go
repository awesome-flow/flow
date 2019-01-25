package devenv

type Fragment interface {
	Extract(*Context) interface{}
}

type DockerComposeFragment string

func (dcf DockerComposeFragment) Extract(context *Context) interface{} {
	return string(dcf)
}
