package log

import (
	"block-crawling/internal/conf"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"runtime"
	"strings"
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

func NewLogger(c *conf.Logger) {
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
