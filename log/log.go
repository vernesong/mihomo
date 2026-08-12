package log

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/observable"

	log "github.com/sirupsen/logrus"
)

var (
	logCh  = make(chan Event)
	source = observable.NewObservable[Event](logCh)
	level  atomic.Int32
)

func init() {
	level.Store(int32(INFO))
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:             true,
		TimestampFormat:           "2006-01-02T15:04:05.000000000Z07:00",
		EnvironmentOverrideColors: true,
	})
}

type Event struct {
	LogLevel LogLevel
	Payload  string
	Time     time.Time
	Fields   []Field
}

type Field struct {
	Key   string
	Value string
}

func (e *Event) Type() string {
	return e.LogLevel.String()
}

func Infoln(format string, v ...any) {
	emit(newLog(INFO, nil, format, v...))
}

func Warnln(format string, v ...any) {
	emit(newLog(WARNING, nil, format, v...))
}

func Errorln(format string, v ...any) {
	emit(newLog(ERROR, nil, format, v...))
}

func Debugln(format string, v ...any) {
	emit(newLog(DEBUG, nil, format, v...))
}

func InfoFields(fields []Field, format string, v ...any) {
	emit(newLog(INFO, fields, format, v...))
}

func WarnFields(fields []Field, format string, v ...any) {
	emit(newLog(WARNING, fields, format, v...))
}

func ErrorFields(fields []Field, format string, v ...any) {
	emit(newLog(ERROR, fields, format, v...))
}

func DebugFields(fields []Field, format string, v ...any) {
	emit(newLog(DEBUG, fields, format, v...))
}

func emit(event Event) {
	logCh <- event
	print(event)
}

func Fatalln(format string, v ...any) {
	log.Fatalf(format, v...)
}

func Subscribe() observable.Subscription[Event] {
	sub, _ := source.Subscribe()
	return sub
}

func UnSubscribe(sub observable.Subscription[Event]) {
	source.UnSubscribe(sub)
}

func Level() LogLevel {
	return LogLevel(level.Load())
}

func SetLevel(newLevel LogLevel) {
	level.Store(int32(newLevel))
}

func print(data Event) {
	if data.LogLevel < Level() {
		return
	}

	switch data.LogLevel {
	case INFO:
		log.Infoln(data.Payload)
	case WARNING:
		log.Warnln(data.Payload)
	case ERROR:
		log.Errorln(data.Payload)
	case DEBUG:
		log.Debugln(data.Payload)
	}
}

func newLog(logLevel LogLevel, fields []Field, format string, v ...any) Event {
	return Event{
		LogLevel: logLevel,
		Payload:  fmt.Sprintf(format, v...),
		Time:     time.Now(),
		Fields:   append([]Field(nil), fields...),
	}
}
