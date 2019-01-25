package devenv

import "sync"

type Context struct {
	sync.Map
}
