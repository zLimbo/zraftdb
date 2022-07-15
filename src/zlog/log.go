package zlog

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

// const ZLogLevel = DebugLevel

var ZLogLevel = InfoLevel
var start time.Time

func init() {
	log.SetFlags(log.Lshortfile)
	// ZLogLevel = DebugLevel
	envLogLevel := os.Getenv("LOG")
	level, _ := strconv.Atoi(envLogLevel)
	ZLogLevel = LogLevel(level)
	start = time.Now()
}

func Log(format string, v ...interface{}) {
	ms := time.Since(start).Milliseconds()
	formatWithTime := fmt.Sprintf("%02d.%03ds| "+format, ms/1000, ms%1000)
	log.Output(3, fmt.Sprintf(formatWithTime, v...))
}

func Debug(format string, v ...interface{}) {
	if ZLogLevel > DebugLevel {
		return
	}
	Log(fmt.Sprintf("DEBUG| "+format, v...))
}

func Info(format string, v ...interface{}) {
	if ZLogLevel > InfoLevel {
		return
	}
	Log(fmt.Sprintf("INFO| "+format, v...))
}

func Warn(format string, v ...interface{}) {
	if ZLogLevel > WarnLevel {
		return
	}
	Log(fmt.Sprintf("WARN| "+format, v...))
}

func Error(format string, v ...interface{}) {
	Log(fmt.Sprintf("ERROR| "+format, v...))
	panic("")
}
