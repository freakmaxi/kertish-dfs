package errors

import "errors"

var (
	ErrQuit = errors.New("operation does not need to continue")
)
