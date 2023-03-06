package log

import (
	"block-crawling/internal/conf"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	stdAtomic "sync/atomic"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var log *zap.SugaredLogger
var logger *zap.Logger

func callerEncoder(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(strings.Join([]string{caller.TrimmedPath(), runtime.FuncForPC(caller.PC).Name()}, ":"))
}

func newLoggerConfig(c *conf.Logger) (loggerConfig zap.Config) {
	if c.DEBUG {
		loggerConfig = zap.NewDevelopmentConfig()
	} else {
		loggerConfig = zap.NewProductionConfig()
	}
	loggerConfig.OutputPaths = []string{c.FileName}
	loggerConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	loggerConfig.EncoderConfig.EncodeCaller = callerEncoder
	var zapLevel zapcore.Level
	switch strings.ToLower(c.Level) {
	case "debug":
		zapLevel = zap.DebugLevel
	case "info":
		zapLevel = zap.InfoLevel
	case "warn":
		zapLevel = zap.WarnLevel
	case "error":
		zapLevel = zap.ErrorLevel
	default:
		zapLevel = zap.InfoLevel
	}
	loggerConfig.Level = zap.NewAtomicLevelAt(zapLevel)
	return
}

func BootstrapLogger(c *conf.Logger) {
	/*if err := config.GetConfig("logger").UnmarshalExact(&c); err != nil {
		panic(errors.Wrap(err, "get config logger error"))
	}*/

	loggerConfig := newLoggerConfig(c)
	if !c.DEBUG {
		loggerConfig.DisableCaller = true
	} else {
		loggerConfig.Development = true
	}

	l, err := loggerConfig.Build(zap.AddCallerSkip(1))
	if err != nil {
		panic(errors.Wrap(err, "error of init logger"))
	}
	logger = l
	log = l.Sugar()
	registerSignal()
}

func Info(msg string, args ...zap.Field) {
	logger.Info(msg, args...)
}

func Debug(msg string, args ...zap.Field) {
	logger.Debug(msg, args...)
}

func Warn(msg string, args ...zap.Field) {
	logger.Warn(msg, args...)
}

func Warne(msg string, err error) {
	logger.Warn(msg, zap.Any("warn", err))
}

func Error(msg string, args ...zap.Field) {
	logger.Error(msg, args...)
}

func Errore(msg string, err error) {
	logger.Error(msg, zap.Any("error", err))
}

func Fatal(msg string, args ...zap.Field) {
	logger.Fatal(msg, args...)
}

func Fatale(msg string, err error) {
	logger.Fatal(msg, zap.Any("error", err))
}

func Panic(msg string, args ...zap.Field) {
	logger.Panic(msg, args...)
}

var enableSignalLog uint32 = 0

func registerSignal() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGUSR1)
	go func() {
		for range ch {
			enabled, ok := toggleSignalLog()
			if !ok {
				Info("TOGGLE SIGNAL LOG FAILED")
				continue
			}

			if enabled {
				Info("SIGNAL LOG HAS BEEN ENABLED")
			} else {
				Info("SIGNAL LOG HAS BEEN DISABLED")
			}
		}
	}()
}

func toggleSignalLog() (enabled, ok bool) {
	if isSignalLogEnabled() {
		return false, stdAtomic.CompareAndSwapUint32(&enableSignalLog, 1, 0)
	}
	return true, stdAtomic.CompareAndSwapUint32(&enableSignalLog, 0, 1)
}

func isSignalLogEnabled() bool {
	return stdAtomic.LoadUint32(&enableSignalLog) == 1
}

// InfoS only show messages when signaled by outside.
func InfoS(msg string, args ...zap.Field) {
	if !isSignalLogEnabled() {
		return
	}
	logger.Info(msg, args...)
}

// DebugS only show messages when signaled by outside.
func DebugS(msg string, args ...zap.Field) {
	if !isSignalLogEnabled() {
		return
	}
	logger.Debug(msg, args...)
}

// WarnS only show messages when signaled by outside.
func WarnS(msg string, args ...zap.Field) {
	if !isSignalLogEnabled() {
		return
	}
	logger.Warn(msg, args...)
}

// WarneS only show messages when signaled by outside.
func WarneS(msg string, err error) {
	if !isSignalLogEnabled() {
		return
	}
	logger.Warn(msg, zap.Any("warn", err))
}

// ErrorS only show messages when signaled by outside.
func ErrorS(msg string, args ...zap.Field) {
	if !isSignalLogEnabled() {
		return
	}
	logger.Error(msg, args...)
}

// ErrorS only show messages when signaled by outside.
func ErroreS(msg string, err error) {
	if !isSignalLogEnabled() {
		return
	}
	logger.Error(msg, zap.Any("error", err))
}

// FatalS only show messages when signaled by outside.
func FatalS(msg string, args ...zap.Field) {
	if !isSignalLogEnabled() {
		return
	}
	logger.Fatal(msg, args...)
}

// FataleS only show messages when signaled by outside.
func FataleS(msg string, err error) {
	if !isSignalLogEnabled() {
		return
	}
	logger.Fatal(msg, zap.Any("error", err))
}

// PanicS only show messages when signaled by outside.
func PanicS(msg string, args ...zap.Field) {
	if !isSignalLogEnabled() {
		return
	}
	logger.Panic(msg, args...)
}
