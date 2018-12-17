package base

import (
	"github.com/op/go-logging"
)

func NewLogger() *logging.Logger{
	return logging.MustGetLogger("xxx")
}
