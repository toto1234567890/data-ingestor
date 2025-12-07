package logger

import (
	"fmt"
	"log"
	"os"
)

// -----------------------------------------------------------------------------

// Logger provides structured logging functionality
type Logger struct {
	name   string
	logger *log.Logger
	config interface{}
}

// -----------------------------------------------------------------------------

// NewLogger creates a new Logger instance
func NewLogger(config interface{}, name string) *Logger {
	l := &Logger{
		name:   name,
		logger: log.New(os.Stdout, "", log.LstdFlags),
		config: config,
	}
	return l
}

// -----------------------------------------------------------------------------

// Info logs informational messages
func (l *Logger) Debug(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	l.logger.Printf("[%s] DEBUG: %s", l.name, msg)
}

// -----------------------------------------------------------------------------

// Info logs informational messages
func (l *Logger) Warning(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	l.logger.Printf("[%s] WARNING: %s", l.name, msg)
}

// -----------------------------------------------------------------------------

// Info logs informational messages
func (l *Logger) Info(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	l.logger.Printf("[%s] INFO: %s", l.name, msg)
}

// -----------------------------------------------------------------------------

// Error logs error messages
func (l *Logger) Error(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	l.logger.Printf("[%s] ERROR: %s", l.name, msg)
}

// -----------------------------------------------------------------------------

// Critical logs critical errors and exits the application
func (l *Logger) Critical(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	l.logger.Printf("[%s] CRITICAL: %s", l.name, msg)
	os.Exit(1)
}
