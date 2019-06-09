package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"runtime"
	"time"
)

type MsgStatus uint8

const (
	MsgStatusNone MsgStatus = iota
	MsgStatusDone
	MsgStatusErr
)

type Message struct {
	body   []byte
	status MsgStatus
	done   chan struct{}
}

func NewMessage(body []byte) *Message {
	return &Message{
		body:   body,
		status: MsgStatusNone,
		done:   make(chan struct{}),
	}
}

func (msg *Message) Await() MsgStatus {
	<-msg.done
	return msg.status
}

func (msg *Message) Complete(status MsgStatus) {
	msg.status = status
	close(msg.done)
}

func (msg *Message) Body() []byte {
	return msg.body
}

type LogSev uint8

const (
	LogSevDebug LogSev = iota
	LogSevTrace
	LogSevInfo
	LogSevWarn
	LogSevError
	LogSevFatal
)

type Log struct {
	sev     LogSev
	payload string
}

func NewLog(sev LogSev, payload string) *Log {
	return &Log{
		sev:     sev,
		payload: payload,
	}
}

type Logger struct {
	logs chan *Log
	out  io.Writer
}

var _ Runner = (*Logger)(nil)

func NewLogger(out io.Writer) *Logger {
	return &Logger{
		logs: make(chan *Log),
		out:  out,
	}
}

func (logger *Logger) Start() error {
	go func() {
		var err error
		for log := range logger.logs {
			_, err = fmt.Fprintln(logger.out, logger.Format(log))
			if err != nil {
				panic(err.Error())
			}
		}
	}()

	return nil
}

func (logger *Logger) Stop() error {
	close(logger.logs)

	return nil
}

var LogSevLex map[LogSev]string

func init() {
	LogSevLex = map[LogSev]string{
		LogSevDebug: "DEBUG",
		LogSevTrace: "TRACE",
		LogSevInfo:  "INFO",
		LogSevWarn:  "WARN",
		LogSevError: "ERROR",
		LogSevFatal: "FATAL",
	}
}
func (logger *Logger) Format(log *Log) string {
	return fmt.Sprintf(
		"%s\t%s\t%s",
		time.Now().Format(time.RFC3339),
		LogSevLex[log.sev],
		log.payload,
	)
}

func (logger *Logger) Debug(payload string) {
	logger.logs <- NewLog(LogSevDebug, payload)
}

func (logger *Logger) Trace(payload string) {
	logger.logs <- NewLog(LogSevTrace, payload)
}

func (logger *Logger) Info(payload string) {
	logger.logs <- NewLog(LogSevInfo, payload)
}

func (logger *Logger) Warn(payload string) {
	logger.logs <- NewLog(LogSevWarn, payload)
}

func (logger *Logger) Error(payload string) {
	logger.logs <- NewLog(LogSevError, payload)
}

func (logger *Logger) Fatal(payload string) {
	logger.logs <- NewLog(LogSevFatal, payload)
}

type Config struct{}

func NewConfig() *Config {
	return &Config{}
}

type Context struct {
	logger *Logger
	config *Config
}

var _ Runner = (*Context)(nil)

func NewContext(config *Config) *Context {
	logger := NewLogger(os.Stdout)
	return &Context{
		logger: logger,
		config: config,
	}
}

func (ctx *Context) Logger() *Logger {
	return ctx.logger
}

func (ctx *Context) Config() *Config {
	return ctx.config
}

func (ctx *Context) Start() error {
	if err := ctx.logger.Start(); err != nil {
		return err
	}
	return nil
}

func (ctx *Context) Stop() error {
	if err := ctx.logger.Stop(); err != nil {
		return err
	}
	return nil
}

type Receiver interface {
	Receive(*Message) error
}

type Connector interface {
	Connect(nthreads int, receiver Receiver) error
}

type Runner interface {
	Start() error
	Stop() error
}

type Dumper struct {
	ctx *Context
	out io.Writer
}

var _ Receiver = (*Dumper)(nil)

func NewDumper(ctx *Context, out io.Writer) *Dumper {
	return &Dumper{
		ctx: ctx,
		out: out,
	}
}

func (d *Dumper) Receive(msg *Message) error {
	_, err := fmt.Fprintf(d.out, "Dumper received a message: %q\n", string(msg.Body()))
	return err
}

type Listener struct {
	ctx   *Context
	bind  string
	done  chan struct{}
	queue chan *Message
	lstnr net.Listener
}

var _ Connector = (*Listener)(nil)
var _ Runner = (*Listener)(nil)

func NewListener(ctx *Context, bind string) *Listener {
	listener := &Listener{
		ctx:   ctx,
		bind:  bind,
		done:  make(chan struct{}),
		queue: make(chan *Message),
	}

	return listener
}

func (listener *Listener) Start() error {
	l, err := net.Listen("tcp", listener.bind)
	if err != nil {
		return err
	}
	listener.lstnr = l

	done := false
	go func() {
		<-listener.done
		done = true
	}()

	go func() {
		for !done {
			listener.ctx.Logger().Debug("Listener is waiting for accept")
			c, err := l.Accept()
			if err != nil {
				listener.ctx.Logger().Error(err.Error())
				continue
			}
			go listener.Handle(c)
		}
		listener.ctx.Logger().Info("Listener is closing")
		l.Close()
	}()

	return nil
}

func (listener *Listener) Handle(c net.Conn) {
	listener.ctx.Logger().Debug(fmt.Sprintf("New connection from %s", c.RemoteAddr()))

	defer c.Close()
	reader := bufio.NewReader(c)
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		msg := &Message{
			body: scanner.Bytes(),
		}
		listener.queue <- msg
	}

	if err := scanner.Err(); err != nil {
		listener.ctx.Logger().Error(err.Error())
	}

	listener.ctx.Logger().Debug(fmt.Sprintf("Done reading from %s", c.RemoteAddr()))
}

func (listener *Listener) Connect(nthreads int, receiver Receiver) error {
	for i := 0; i < nthreads; i++ {
		go func() {
			for msg := range listener.queue {
				if err := receiver.Receive(msg); err != nil {
					listener.ctx.Logger().Error(err.Error())
				}
			}
		}()
	}
	return nil
}

func (listener *Listener) Stop() error {
	close(listener.done)

	return nil
}

type Grep struct {
	ctx    *Context
	queue  chan *Message
	lookup string
}

var _ Receiver = (*Grep)(nil)
var _ Connector = (*Grep)(nil)

func NewGrep(ctx *Context, lookup string) *Grep {
	return &Grep{
		ctx:    ctx,
		queue:  make(chan *Message),
		lookup: lookup,
	}
}

func (g *Grep) Connect(nthreads int, receiver Receiver) error {
	for i := 0; i < nthreads; i++ {
		go func() {
			for msg := range g.queue {
				if err := receiver.Receive(msg); err != nil {
					g.ctx.Logger().Error(err.Error())
				}
			}
		}()
	}
	return nil
}

func (g *Grep) Receive(msg *Message) error {
	if g.doGrep(msg) {
		g.queue <- msg
	}
	return nil
}

func (g *Grep) doGrep(msg *Message) bool {
	match := bytes.Contains(msg.Body(), []byte(g.lookup))
	if !match {
		g.ctx.Logger().Debug(fmt.Sprintf("Message %q was filtered out by grep", msg.Body()))
	}

	return match
}

type Failer func() error

func ExecEnsure(failers ...Failer) error {
	for _, failer := range failers {
		if err := failer(); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	nthreads := runtime.GOMAXPROCS(-1)

	config := NewConfig()
	ctx := NewContext(config)

	listener := NewListener(ctx, ":10000")
	grep := NewGrep(ctx, "llo")
	dumper := NewDumper(ctx, os.Stdout)

	if err := ExecEnsure(
		func() error { return listener.Connect(nthreads, grep) },
		func() error { return grep.Connect(nthreads, dumper) },
		ctx.Start,
		listener.Start,
	); err != nil {
		panic(err.Error())
	}

	ctx.Logger().Info("Successfully started listener")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	ctx.Logger().Info("Terminating the program")

	if err := ExecEnsure(
		listener.Stop,
		ctx.Stop,
	); err != nil {
		panic(err.Error())
	}

	os.Exit(0)
}
