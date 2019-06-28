package corev1alpha1

import (
	"fmt"
	"io"
	"os"
	"time"
)

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
	done chan struct{}
}

var _ Runner = (*Logger)(nil)

func NewLogger(out io.Writer) *Logger {
	return &Logger{
		logs: make(chan *Log),
		out:  out,
		done: make(chan struct{}),
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
		close(logger.done)
	}()

	return nil
}

func (logger *Logger) Stop() error {
	close(logger.logs)
	<-logger.done

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

func (logger *Logger) Debug(format string, a ...interface{}) {
	logger.logs <- NewLog(LogSevDebug, fmt.Sprintf(format, a...))
}

func (logger *Logger) Trace(format string, a ...interface{}) {
	logger.logs <- NewLog(LogSevTrace, fmt.Sprintf(format, a...))
}

func (logger *Logger) Info(format string, a ...interface{}) {
	logger.logs <- NewLog(LogSevInfo, fmt.Sprintf(format, a...))
}

func (logger *Logger) Warn(format string, a ...interface{}) {
	logger.logs <- NewLog(LogSevWarn, fmt.Sprintf(format, a...))
}

func (logger *Logger) Error(format string, a ...interface{}) {
	logger.logs <- NewLog(LogSevError, fmt.Sprintf(format, a...))
}

func (logger *Logger) Fatal(format string, a ...interface{}) {
	logger.logs <- NewLog(LogSevFatal, fmt.Sprintf(format, a...))
	logger.Stop()
	logger.terminate()
}

func (logger *Logger) terminate() {
	os.Exit(1)
}
