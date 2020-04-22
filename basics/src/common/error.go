package common

import (
	"encoding/json"
	"io"
)

type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func NewError(code int, message string) Error {
	return Error{
		Code:    code,
		Message: message,
	}
}

func NewErrorFromReader(reader io.Reader) Error {
	var e Error
	if err := json.NewDecoder(reader).Decode(&e); err != nil {
		return Error{
			Code:    999,
			Message: err.Error(),
		}
	}
	return e
}
