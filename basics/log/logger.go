package log

import (
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewLogger(service string) (*zap.Logger, bool) {
	logType := os.Getenv("LOGGING_TYPE")
	if len(logType) == 0 {
		logType = "text" // json
	}

	logOutput := os.Getenv("LOGGING_OUTPUT")
	if len(logOutput) == 0 {
		logOutput = "console" // file
	}

	logConfig := zap.NewDevelopmentEncoderConfig()

	logEncoder := zapcore.NewConsoleEncoder(logConfig)
	if strings.Compare(logType, "json") == 0 {
		logEncoder = zapcore.NewJSONEncoder(logConfig)
	}

	logLock := zapcore.Lock(os.Stdout)
	if strings.Compare(logOutput, "file") == 0 {
		logTarget := os.Getenv("LOGGING_TARGET")
		if len(logTarget) == 0 {
			logTarget = "/var/log"
		}

		logPath := path.Join(logTarget, fmt.Sprintf("kertish-dfs-%s", service))
		if err := os.MkdirAll(logPath, 0777); err != nil {
			fmt.Printf("ERROR: Unable to create logging path: %s", err.Error())
			os.Exit(1)
		}
		logPath = path.Join(logPath, fmt.Sprintf("%s.log", time.Now().Format("since-20060102")))
		file, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			fmt.Printf("ERROR: Unable to create logging file: %s", err.Error())
			os.Exit(2)
		}
		logLock = zapcore.Lock(file)
	}

	logLevel := os.Getenv("LOGGING_LEVEL")
	zapLevel := zapcore.InfoLevel
	switch strings.ToLower(logLevel) {
	case "error":
		zapLevel = zapcore.ErrorLevel
	case "warn":
		zapLevel = zapcore.WarnLevel
	default:
	}

	logCore := zapcore.NewCore(logEncoder, logLock, zapLevel)
	return zap.New(zapcore.NewTee(logCore)), strings.Compare(logOutput, "file") != 0
}
