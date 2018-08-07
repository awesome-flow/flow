# Msgrelay and Flow Framework

## Intro

Msgrelay (stands for message relay) is a modular highly configurable sidecar for
fast and reliable real-time data transmission.

This software provides an extendable framework which allows one to build data
pipelines, fan-outs, proxies, multiplexers and so on.

In the heart of this software there is a concept of interconnected reusable
links with a well-defined purpose. Links are building blocks for the pipeline.
Links have in- and out- connectors. Distinct types of connectors are described
in #Connectors paragraph.

A single sidecar runs and controls exactly one pipeline. We call this framework
"Flow".

## Building a Pipeline

The framework provides a set of primitive building blocks (e.g.: http-receiver,
udp-sink, router, multiplexer). It all starts with the definition of the
pipeline components. One-by-one, the `components` block of a yaml config file
defines main properties and settings for the components.

Once the components are defined, `pipeline` block defines relationships between
these components and determines the flow of the data.

## Connectors

The idea of links and connectors comes from engineering programming software
and is mostly inspired by LabView.

Links might be of 3 major types:
  * one-to-one
  * one-to-many
  * many-to-one

```
  One-to-one    One-to-many    Many-to-one
      |              |             \|/ 
      O              O              O
      |             /|\             |
```

Links that receive messages from the outer world are called receivers (e.g.:
http, udp, tcp servers) They initiate message lifecycle. The ones that send
messages to outerworld are called sinks (kafka, tcp, udp). Sink stage denotes
message lifecycle termination. Message lifecycle stages are explained in 
Messages part.

We call them none-to-one and one-to-none links.

```
  Receiver    Sink
     O         |
     |         O
```

A pipeline is defined using these 3 basic types of links. Links define
corresponding methods in order to expose connectors:
  * `ConnectTo(flow.Link)`
  * `LinkTo([]flow.Link)`
  * `RouteTo(map[string]flow.Link)`

Here comes one important remark about connectors: `RouteTo` defines OR logic:
where a message is being dispatched to at most 1 link (therefore the connectors
are named using keys, but the message is never replicated). `LinkTo`, on the
opposite size, defines AND logic: a message is being dispatched to 0 or more
links (message is replicated).

## Connector API

TODO

## Links

Flow core comes with a set of primitive links which might be a use in the
majority of basic pipelines. More complex links are being built using these
blocks and enreaching the standard ones.

### Core Links:

#### Receivers:

  * `receiver.http`: a none-to-one link, HTTP receiver server
  * `receiver.tcp`: a none-to-one link, TCP receiver server
  * `receiver.udp`: a none-to-one link, UDP receiver server
  * `receiver.unix`: a non-to-one link, UNIX socket server

#### Intermediate Links:

  * `links.demultiplexer`: a many-to-one link, collects messages from N(>=1)
    links and pipes them in a single channel
  * `links.multiplexer`: a one-to-many link, multiplexes copies of messages
    to N(>=0) links and reports the composite status back.
  * `links.buffer`: a one-to-one link, implements an intermediate buffer with
    lightweight retry logic.
  * `links.router`: a one-to-many link, sends messages to at most 1 link based
    on the message meta attributes (this attribute is configurable).
  * `links.fanout`: a one-to-many link, sends messages to exactly 1 link,
    changing destination after every submission like a roller.
  * `links.throttler`: a one-to-one link, implements rate limiting
    functionality.

#### Sinks:

  * `sink.dumper`: a one-to-none link, dumps messages into a file (including
    STDOUT and STDERR).
  * `sink.tcp`: a one-to-none link, sends messages to a TCP endpoint
  * `sink.udp`: a one-to-none link, sends messages to a UDP endpoint
  * `sink.kafka`: a one-to-none link, sends messages to kafka (currently using
    Sarama library).

## Messages

msgrelay is supposed to pass messages. From the user perspective, a message is
a binary payload with a set of key-value metainformation tied with it.

Internally, messages are stateful. Message initiator can subscribe to message
updates. Pipeline links pass messages top-down. Every link can stop message
propagation immediately and finalize it. Message termination notification
bubbles up to it's initiator (this mechanism is being used for synchronous
message submission: when senders can report the exact submission status back).

```
  Message lifecycle
  +-----------------+
  | message created |  < . . . . .
  +-----------------+            .
           |  <-------+          .
           V          |          .
  +----------------+  |          .
  | passed to link |  | N times  .
  +----------------+  |          .
           |          |          .
           +----------+          .
           |                     . Ack
           V                     .
        +------+                 .
        | sink |                 .
        +------+                 .
           |                     .
           V                     .
     +-----------+               .
     | finalized | . . . . . . . .
     +-----------+
```

## The intermediate loop of responsibility

Links like multiplexer (MPX) multiply messages to 0 or more links and report the
composite status. In order to send the accurate submission status back, they
implement behavior which we call intermediate responsibility. It means these
links behave like implicit message producers and subscribe to notifications
from all messages they emitted.

Once all multiplexed messages have notified their submission status (or a
timeout fired), the link reports back the composite status update: it might be
a timeout, a partial send status, a total failure of a total success. For the
upstream links this behavior is absolutely invisible and they only receive the
original message status update.

```
  The intermediate loop of responsibility

               +----------+
               | Producer | < .
               +----------+   . Composite
                     |        . status 
                     V        . update
                  +-----+ . . .
                  | MPX |
    . . . . . >   +-----+    < . . . . . 
    .               /|\                .
    .             /  |  \              . Individual
    .           /    |    \            . status
    .         /      |      \          . update
    . +-------+  +-------+  +--------+ .
      | Link1 |  | Link2 |  | Link 3 |
      +-------+  +-------+  +--------+
```

## Message Status Updates

A message reports it's status exactly once. Once the message has reported it's
submission status, it's finalized: none to be done with this message anymore.

Message statuses are pre-defined:

* `MsgStatusNew`: In-flight status.
* `MsgStatusDone`: Full success status.
* `MsgStatusPartialSend`: Parital success.
* `MsgStatusInvalid`: Message processing terminated due to an external error
  (wrong message).
* `MsgStatusFailed`: Message processing terminated due to an internal error.
* `MsgStatusTimedOut` Message processing terminated due to a timeout.
* `MsgStatusUnroutable` Message type or destination is unknown.
* `MsgStatusThrottled` Message processing terminated due to an internal rate
  limits.

## Pipeline commands

Sometimes there might be a need of sending control signals to components. If a
component is intended to react to these signals, it overrides method called 
`ExecCmd(*flow.Cmd) error`. If a component keeps some internal hierarchy of
links, it can use the same API and send custom commands.

It's the pipeline that keeps knowledge of the component hierarchy and it
represents it as a tree internally. Commands propagate either top-down or
bottom-up. Pipeline implements method `ExecCmd(*flow.Cmd, flow.CmdPropagation)`.

The second argument indicates the direction in which a command would be
propagated. Say, pipeline start command should take effect bottom-up: receivers
should be activated last. On the other hand, stopping the pipeline should be
applied top-down as deactivating receivers allows to flush messages in flight
safely.

flow.Cmd is a structure, not just a constant for reasons: it allows one to
extend command instances by attaching a payload.

Flow command constants are named:
  * `CmdCodeStart`
  * `CmdCodeStop`

## Modularity and Plugin Infrastructure

msgrelay is a widely extendable software due to the plugin system. We use
Golang plugins in order to let developers create their custom links. A plugin
must conform to the same interface as the core links: expose a constructor that
will produce a new instance of the link. One is allowed to implement any kind
of link: receivers, links, sinks, multiplexers, demultiplexers, etc.

In order to link a plugin, one describes the component in the config file like:

```yaml
components:
  <link_name>:
    plugin: <plugin_name>
    constructor: <ConstrFunc>
    params:
      ...
```

`link_name` is the same concept as naming the core links: same plugin might be
instantiated as many times as needed under distinct names.

`plugin_name` (provided with no angle braces) is the name of the plugin. The
name of the plugin includes naming convention: it would be mapped to the real
file lookup path.

By default, plugins are expected to be found in folder called
`/etc/msgrelay/plugins`, but is configurable by specifying `FLOW_PLUGIN_PATH`
environment variable.

A structure of a plugin folder looks like:

```
/etc/msgrelay/plugins
└── plugin_name
    ├── plugin_name.go
    ├── plugin_name.so
    └── plugin_name_test.go
```

The .go files are pretty trivial with some minor remarks we will provide a bit
later.

The .so file is being created by go build once run with `-buildmode=plugin`.
For more details see [Golang plugin reference](https://golang.org/pkg/plugin/).

A plugin must be built for the same archetecture and with the same release of
Go. Frankly speaking, Go plugin ecosystem is pretty fragile on Darwin
architecture yet (the progres is quite promising as there is a great interest
in the community). Also, building your program with `GODEBUG=cgocheck=2` will
crash once you import plugin module (it drives go checkers crazy due to passing
Go pointers by non-Go runtime of shared object libraries). This is why we
strongly encourage developing and testing plugins on AMD64 architecture.

Below there is an example of a plugin

```go
package main

import (
	"booking/msgrelay/flow"
	"bufio"
	"fmt"
	"os"
)

type Stdout struct {
	Name   string
	buffer *bufio.Writer
	*flow.Connector
}

func NewStdout(name string, params flow.Params) (flow.Link, error) {
	writer := bufio.NewWriter(os.Stdout)
	return &Stdout{name, writer, flow.NewConnector()}, nil
}

func (s *Stdout) Recv(msg *flow.Message) error {
	s.buffer.Write([]byte(fmt.Sprintf("Message:\n"+
		"    meta: %+v\n"+
		"    payload: %s\n", msg.Meta, msg.Payload)))
	if flushErr := s.buffer.Flush(); flushErr != nil {
		return msg.AckFailed()
	}
	return msg.AckDone()
}

func main() {}
func init() {}
```

The major difference with regular links defined by flow core is:
  * package name is main
  * function main() is there to satisfy Go requirements
  * function init() is there to perform a static bootstrap (called once on
    plugin load)

## Copyright

This software is created by Oleg Sidorov in 2018. It uses some ideas and code
samples written by Ivan Kruglov and Damian Gryski and is mostly inspired by
their work.

This software is distributed under under MIT license. See LICENSE file for full
license text.