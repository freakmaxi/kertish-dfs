package errors

import "errors"

var (
	ErrNoSpace         = errors.New("no space on clusters")
	ErrNoAvailableNode = errors.New("no available node for clusters")
	ErrCreate          = errors.New("not possible to create shadow on data node")
	ErrLock            = errors.New("path/file is locked")
)
