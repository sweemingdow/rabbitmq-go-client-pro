package rm_log

import (
	"log"
	"strings"
	"sync/atomic"
)

type LogLevel uint8

var (
	SilenceLevel LogLevel = 0
	TraceLevel   LogLevel = 1
	DebugLevel   LogLevel = 2
	InfoLevel    LogLevel = 3
	WarnLevel    LogLevel = 4
	ErrorLevel   LogLevel = 5
	FatalLevel   LogLevel = 6

	desc2ll = map[string]LogLevel{
		"silence": SilenceLevel,
		"trace":   TraceLevel,
		"debug":   DebugLevel,
		"info":    InfoLevel,
		"warn":    WarnLevel,
		"error":   ErrorLevel,
		"fatal":   FatalLevel,
	}
)

var (
	logger    MqLogger
	defLogger MqLogger
	logLevel  atomic.Uint32
)

func init() {
	defLogger = stdMqLogger{
		leve2desc: map[LogLevel]string{
			TraceLevel: "[TRACE]",
			DebugLevel: "[DEBUG]",
			InfoLevel:  "[INFO]",
			WarnLevel:  "[WARN]",
			ErrorLevel: "[ERROR]",
			FatalLevel: "[FATAL]",
		},
	}
}

type MqLogger interface {
	MqLog(ll LogLevel, content string)
}

func InitLogger(li MqLogger) {
	logger = li
}

// eg: debug, Debug, DEBUG, deBuG both ok
func SetLoggerLevelHuman(level string) bool {
	if ll, ok := desc2ll[strings.ToLower(level)]; ok {
		SetLoggerLevel(ll)
		return true
	}

	return false
}

func SetLoggerLevel(ll LogLevel) {
	if ll > ErrorLevel {
		ll = ErrorLevel
	}

	logLevel.Store(uint32(ll))
}

func doLog(ll LogLevel, content string) {
	if !can(ll) {
		return
	}

	var mqLogger = logger
	if mqLogger == nil {
		mqLogger = defLogger
	}

	mqLogger.MqLog(ll, content)
}

func can(ll LogLevel) bool {
	return ll >= LogLevel(logLevel.Load())
}

func CanTrace() bool {
	// save content construction overhead (fmt.Sprintf(...))
	return TraceLevel >= LogLevel(logLevel.Load())
}

func CanDebug() bool {
	// save content construction overhead (fmt.Sprintf(...))
	return DebugLevel >= LogLevel(logLevel.Load())
}

func Trace(content string) {
	doLog(TraceLevel, content)
}

func Debug(content string) {
	doLog(DebugLevel, content)
}

func Info(content string) {
	doLog(InfoLevel, content)
}

func Warn(content string) {
	doLog(WarnLevel, content)
}

func Error(content string) {
	doLog(ErrorLevel, content)
}

func Fatal(content string) {
	doLog(FatalLevel, content)
}

type stdMqLogger struct {
	leve2desc map[LogLevel]string
}

func (sl stdMqLogger) MqLog(ll LogLevel, content string) {
	log.Printf("%s %s\n", sl.leve2desc[ll], content)
}
