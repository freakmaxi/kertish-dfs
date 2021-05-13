package common

import (
	"encoding/json"
	"io"
)

// Error struct is to hold and export/serialize the action result concluded with an error
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// NewError initialises the new Error base on the given details
func NewError(code int, message string) Error {
	return Error{
		Code:    code,
		Message: message,
	}
}

// NewErrorFromReader creates the Error struct by reading the data from stream source (ex: Http Request Result)
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
