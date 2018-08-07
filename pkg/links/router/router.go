package links

import (
	"booking/bmetrics"
	"booking/msgrelay/flow"
	"fmt"
	"sync"

	"gitlab.booking.com/go/tell"
)

type Router struct {
	Name        string
	routingFunc flow.RoutingFunc
	routes      map[string]flow.Link
	*flow.Connector
	*sync.Mutex
}

func NewRouter(name string, params flow.Params) (flow.Link, error) {
	routingKey, ok := params["routing_key"]
	if !ok {
		return nil, fmt.Errorf("Router %s parameters are missing routing_key", name)
	}
	var routingFunc flow.RoutingFunc
	if strKey, ok := routingKey.(string); ok {
		errNoKey := fmt.Errorf("Message is missing routing key %s", strKey)
		routingFunc = func(msg *flow.Message) (string, error) {
			k, ok := msg.Meta[strKey]
			if !ok {
				return "", errNoKey
			}
			return k, nil
		}
	} else if funcKey, ok := routingKey.(func(*flow.Message) (string, error)); ok {
		routingFunc = funcKey
	} else {
		return nil, fmt.Errorf("Incompatible routing_key type")
	}
	routes := make(map[string]flow.Link)
	r := &Router{name, routingFunc, routes, flow.NewConnector(), &sync.Mutex{}}
	go r.dsptchMsgs()
	return r, nil
}

func (r *Router) RouteTo(routes map[string]flow.Link) error {
	r.Lock()
	defer r.Unlock()
	for routeKey, routeDst := range routes {
		r.routes[routeKey] = routeDst
	}
	return nil
}

func (r *Router) AddRoute(routeKey string, routeDst flow.Link) error {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.routes[routeKey]; ok {
		return fmt.Errorf("Route under key %s already exists. Drop it first", routeKey)
	}
	r.routes[routeKey] = routeDst
	return nil
}

func (r *Router) DropRoute(routeKey string) (flow.Link, error) {
	r.Lock()
	defer r.Unlock()
	routeDst, ok := r.routes[routeKey]
	if !ok {
		return nil, fmt.Errorf("Router under key %s is undefined", routeKey)
	}
	delete(r.routes, routeKey)
	return routeDst, nil
}

func (r *Router) GetRoutes() map[string]flow.Link {
	return r.routes
}

func (r *Router) ConnectTo(flow.Link) error {
	panic("Router is not supposed to be connected directly")
}

func (r *Router) Recv(msg *flow.Message) error {
	return r.Send(msg)
}

func (r *Router) dsptchMsgs() {
	for msg := range r.GetMsgCh() {
		dst, dstErr := r.routingFunc(msg)
		if dstErr != nil {
			msg.AckUnroutable()
			return
		}
		if route, ok := r.routes[dst]; ok {
			bmetrics.GetOrRegisterCounter("links", "router", "dst_"+dst).Inc(1)
			route.Recv(msg)
		} else {
			bmetrics.GetOrRegisterCounter("links", "router", "unknown").Inc(1)
			tell.Warnf("Unknown destination: [%s]", dst)
			msg.AckUnroutable()
		}
	}
}
