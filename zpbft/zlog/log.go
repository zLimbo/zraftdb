package zlog

import (
	"fmt"
	"log"
)

type LogLevel = int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

func init() {
	log.SetFlags(log.Lshortfile)
}

var kLevel LogLevel

func SetLevel(level LogLevel) {
	kLevel = level
}

func Debug(format string, v ...interface{}) {
	if kLevel > DebugLevel {
		return
	}
	log.Output(2, fmt.Sprintf("Debug| "+format, v...))
}

func Info(format string, v ...interface{}) {
	if kLevel > InfoLevel {
		return
	}
	log.Output(2, fmt.Sprintf("\033[32m"+"INFO| "+format+"\033[0m", v...))
}

func Warn(format string, v ...interface{}) {
	if kLevel > WarnLevel {
		return
	}
	log.Output(2, fmt.Sprintf("\033[33m"+"WARN| "+format+"\033[0m", v...))
}

func Error(format string, v ...interface{}) {
	s := fmt.Sprintf("\033[31m"+"ERROR| "+format+"\033[0m", v...)
	log.Output(2, s)
	panic(s)
}
