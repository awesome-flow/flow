# [WIP] The Flow Framework

![logo](https://github.com/whiteboxio/flow/blob/master/flow.png)

## Intro

The Flow framework is a comprehensive library of primitive building blocks
and tools that lets one design and build data relays of any comlexity. Highly
inspired by electrical circuit elements design, it provides a clear and
well-defined approach to building message pipelines of any nature. One can think
of Flow as LEGO in the world of data: a set of primitive reusable building
bricks which are gathered together in a sophisticated assembly.

Flow can be a great fit in a SOA environment. It's primitives can be combined
with a service discovery solution, external config provider etc; it can plug a
set of security checks and obfuscation rules, perform an in-flight dispatching,
implement a complex aggregation logic and so on. It can also be a good
replacement for existing sidecars: it's high performance, modularity and the
plugin system allows one to solve nearly any domain-specific messaging problem.

The ultimate goal of Flow is to turn a pretty complex low-level software problem
into a logical map of data transition and transformation elements. There exists
an extensive list of narrow-scoped relays, each one of them is dedicated to
solve it's very own problem. In a bigger infrastructure it normally turns into a
necessity of supporting a huge variety of daemons and sidecars, their custom
orchestration recipes and a limitation of knowledge sharing. Flow is solving
these problems by unifying the approach, making the knowledge base generic
and transferable  and by shifting developer's minds from low-level engineering
and/or system administration problem towards a pure business-logic decision
making process.

## Concepts

Flow comes with a very compact dictionary or terms which are widely used in this
documentation.

First of all, Flow is here to pass some data around. A unit of data is a *message*.
Every Flow program is a singular *pipeline*, which is built of primitives: we call
them *links*. An example of a link: UDP receiver, router, multiplexer, etc. Links
are connectable to each other, and the connecting elements are called *connectors*.
Connectors are mono-directional: they pass messages in one direction from
link A to link B. In this case we say that A has an *outcoming connector*, an B
has an *incoming connector*.

Links come with the semantics of connectability: some of them can have outcoming
connectors only: we call them out-links, or *receivers*, and some can hve
incoming connectors only: in-links, or *sinks*. A receiver is a link that
receives internal messages: a network listener, pub-sub client etc. They ingest
messages into the pipeline. A sink has the opposite purpose: to send messages
somewhere else. This is where the lifecycle of the message ends. An example
of a sink: an HTTP sender, Kafka ingestor, log file dumper, etc. A pipeline
is supposed to start with one or more receivers and end up with one or more
sinks. Generic in-out links are supposed to be placed in the middle of the
pipeline.

Links are gathered in a chain of isolated self-contained elements. Every link
has a set of methods to receive and pass messages. The custom logic is
implemented inside a link body. A link knows nothing about it's neighbours and
should avoid any neighbour-specific logic.

===== The text below this line is unedited and might contain outdated info =====

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

flowd is supposed to pass messages. From the user perspective, a message is
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

See [Flow plugins](https://github.com/whiteboxio/flow-plugins).

## Copyright

This software is created by Oleg Sidorov in 2018. It uses some ideas and code
samples written by Ivan Kruglov and Damian Gryski and is mostly inspired by
their work.

This software is distributed under under MIT license. See LICENSE file for full
license text.
