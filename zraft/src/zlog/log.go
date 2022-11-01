package zlog

import (
	"fmt"
	"log"
	"time"
)

type LogLevel = int

const (
	TraceLevel LogLevel = iota
	DebugLevel
	InfoLevel
	WarnLevel
	ErrorLevel
)

// const kLevel = TraceLevel
// const kLevel = DebugLevel
// const kLevel = InfoLevel

// const kLevel = WarnLevel

var kLevel LogLevel

func SetLevel(level LogLevel) {
	kLevel = level
}

var start time.Time

func init() {
	log.SetFlags(log.Lshortfile)
	start = time.Now()
}

func Log(format string, v ...interface{}) {
	ms := time.Since(start).Milliseconds()
	formatWithTime := fmt.Sprintf("%02d.%03ds| "+format, ms/1000, ms%1000)
	log.Output(3, fmt.Sprintf(formatWithTime, v...))
}

func Trace(format string, v ...interface{}) {
	if kLevel > TraceLevel {
		return
	}
	Log(fmt.Sprintf("Trace| "+format, v...))
}

func Debug(format string, v ...interface{}) {
	if kLevel > DebugLevel {
		return
	}
	Log(fmt.Sprintf("DEBUG| "+format, v...))
}

func Info(format string, v ...interface{}) {
	if kLevel > InfoLevel {
		return
	}
	Log(fmt.Sprintf("INFO| "+format, v...))
}

func Warn(format string, v ...interface{}) {
	if kLevel > WarnLevel {
		return
	}
	Log(fmt.Sprintf("WARN| "+format, v...))
}

func Error(format string, v ...interface{}) {
	Log(fmt.Sprintf("ERROR| "+format, v...))
	panic("")
}
