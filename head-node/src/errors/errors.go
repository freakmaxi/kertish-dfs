package errors

import "errors"

var (
	ErrNoSpace         = errors.New("no space on clusters")
	ErrNoAvailableNode = errors.New("no available node for clusters")
	ErrCreate          = errors.New("not possible to create shadow on data node")
	ErrLock            = errors.New("path/file is locked")
	ErrZombie          = errors.New("file is zombie")
	ErrJoinConflict    = errors.New("joining source folders will have conflict in target")
)
