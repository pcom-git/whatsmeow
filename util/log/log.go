// Copyright (c) 2021 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package waLog contains a simple logger interface used by the other whatsmeow packages.
package waLog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Logger is a simple logger interface that can have subloggers for specific areas.
type Logger interface {
	Warnf(msg string, args ...any)
	Errorf(msg string, args ...any)
	Infof(msg string, args ...any)
	Debugf(msg string, args ...any)
	Sub(module string) Logger
}

type noopLogger struct{}

func (n *noopLogger) Errorf(_ string, _ ...any) {}
func (n *noopLogger) Warnf(_ string, _ ...any)  {}
func (n *noopLogger) Infof(_ string, _ ...any)  {}
func (n *noopLogger) Debugf(_ string, _ ...any) {}
func (n *noopLogger) Sub(_ string) Logger       { return n }

// Noop is a no-op Logger implementation that silently drops everything.
var Noop Logger = &noopLogger{}

type stdoutLogger struct {
	mod   string
	color bool
	min   int
}

var colors = map[string]string{
	"INFO":  "\033[36m",
	"WARN":  "\033[33m",
	"ERROR": "\033[31m",
}

var levelToInt = map[string]int{
	"":      -1,
	"DEBUG": 0,
	"INFO":  1,
	"WARN":  2,
	"ERROR": 3,
}

var fileLog = struct {
	sync.Mutex
	file   *os.File
	date   string
	warned bool
}{}

func (s *stdoutLogger) outputf(level, msg string, args ...any) {
	if levelToInt[level] < s.min {
		return
	}
	var colorStart, colorReset string
	if s.color {
		colorStart = colors[level]
		colorReset = "\033[0m"
	}
	now := time.Now()
	message := fmt.Sprintf(msg, args...)
	timestamp := now.Format("15:04:05.000")
	fmt.Printf("%s%s [%s %s] %s%s\n", timestamp, colorStart, s.mod, level, message, colorReset)
	writeLogFile(now, fmt.Sprintf("%s [%s %s] %s\n", timestamp, s.mod, level, message))
}

func writeLogFile(now time.Time, line string) {
	fileLog.Lock()
	defer fileLog.Unlock()

	file, err := getLogFile(now)
	if err != nil {
		if !fileLog.warned {
			fmt.Fprintf(os.Stderr, "failed to open whatsmeow log file: %v\n", err)
			fileLog.warned = true
		}
		return
	}
	if _, err = file.WriteString(line); err != nil && !fileLog.warned {
		fmt.Fprintf(os.Stderr, "failed to write whatsmeow log file: %v\n", err)
		fileLog.warned = true
	}
}

func getLogFile(now time.Time) (*os.File, error) {
	date := now.Format("20060102")
	if fileLog.file != nil && fileLog.date == date {
		return fileLog.file, nil
	}
	if fileLog.file != nil {
		_ = fileLog.file.Close()
		fileLog.file = nil
		fileLog.date = ""
	}

	logDir := filepath.Join(".", "runtime", "logs")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}
	logPath := filepath.Join(logDir, fmt.Sprintf("whatsmeow_log_%s.txt", date))
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	fileLog.file = file
	fileLog.date = date
	return file, nil
}

func (s *stdoutLogger) Errorf(msg string, args ...any) { s.outputf("ERROR", msg, args...) }
func (s *stdoutLogger) Warnf(msg string, args ...any)  { s.outputf("WARN", msg, args...) }
func (s *stdoutLogger) Infof(msg string, args ...any)  { s.outputf("INFO", msg, args...) }
func (s *stdoutLogger) Debugf(msg string, args ...any) { s.outputf("DEBUG", msg, args...) }
func (s *stdoutLogger) Sub(mod string) Logger {
	return &stdoutLogger{mod: fmt.Sprintf("%s/%s", s.mod, mod), color: s.color, min: s.min}
}

// Stdout is a simple Logger implementation that outputs to stdout. The module name given is included in log lines.
//
// minLevel specifies the minimum log level to output. An empty string will output all logs.
//
// If color is true, then info, warn and error logs will be colored cyan, yellow and red respectively using ANSI color escape codes.
func Stdout(module string, minLevel string, color bool) Logger {
	return &stdoutLogger{mod: module, color: color, min: levelToInt[strings.ToUpper(minLevel)]}
}
