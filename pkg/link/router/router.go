package link

import (
	"fmt"
	"sync"

	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/metrics"
)

type Router struct {
	Name        string
	routingFunc core.RoutingFunc
	routes      map[string]core.Link
	*core.Connector
	*sync.Mutex
}

func New(name string, params core.Params, context *core.Context) (core.Link, error) {
	routingKey, ok := params["routing_key"]
	if !ok {
		return nil, fmt.Errorf("Router %s parameters are missing routing_key", name)
	}
	var routingFunc core.RoutingFunc
	if strKey, ok := routingKey.(string); ok {
		errNoKey := fmt.Errorf("Message is missing the routing key %s", strKey)
		routingFunc = func(msg *core.Message) (string, error) {
			k, ok := msg.Meta(strKey)
			if !ok {
				return "", errNoKey
			}
			return k.(string), nil
		}
	} else if funcKey, ok := routingKey.(func(*core.Message) (string, error)); ok {
		routingFunc = funcKey
	} else {
		return nil, fmt.Errorf("Incompatible routing key type")
	}
	routes := make(map[string]core.Link)
	r := &Router{name, routingFunc, routes, core.NewConnector(), &sync.Mutex{}}

	for _, ch := range r.GetMsgCh() {
		go func(ch chan *core.Message) {
			r.dsptchMsgs(ch)
		}(ch)
	}

	return r, nil
}

func (r *Router) RouteTo(routes map[string]core.Link) error {
	r.Lock()
	defer r.Unlock()
	for routeKey, routeDst := range routes {
		r.routes[routeKey] = routeDst
	}
	return nil
}

func (r *Router) AddRoute(routeKey string, routeDst core.Link) error {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.routes[routeKey]; ok {
		return fmt.Errorf("Route under key %s already exists. Drop it first", routeKey)
	}
	r.routes[routeKey] = routeDst
	return nil
}

func (r *Router) DropRoute(routeKey string) (core.Link, error) {
	r.Lock()
	defer r.Unlock()
	routeDst, ok := r.routes[routeKey]
	if !ok {
		return nil, fmt.Errorf("Router under key %s is undefined", routeKey)
	}
	delete(r.routes, routeKey)
	return routeDst, nil
}

func (r *Router) GetRoutes() map[string]core.Link {
	return r.routes
}

func (r *Router) ConnectTo(core.Link) error {
	panic("Router is not supposed to be connected directly")
}

func (r *Router) Recv(msg *core.Message) error {
	return r.Send(msg)
}

func (r *Router) dsptchMsgs(ch chan *core.Message) {
	for msg := range ch {
		dst, dstErr := r.routingFunc(msg)
		if dstErr != nil {
			msg.AckUnroutable()
			return
		}
		if route, ok := r.routes[dst]; ok {
			metrics.GetCounter("links.router.dst_" + dst).Inc(1)
			route.Recv(msg)
		} else {
			metrics.GetCounter("links.router.unknown").Inc(1)
			msg.AckUnroutable()
		}
	}
}
