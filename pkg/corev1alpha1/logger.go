package corev1alpha1

import (
	"fmt"
	"io"
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
