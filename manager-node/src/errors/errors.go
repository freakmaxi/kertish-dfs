package errors

import "errors"

var (
	ErrExists          = errors.New("cluster is already exists")
	ErrPing            = errors.New("node is not reachable")
	ErrJoin            = errors.New("node joining to cluster is failed")
	ErrMode            = errors.New("setting node mode is failed")
	ErrLastNode        = errors.New("last node of the cluster can not be removed")
	ErrRegistered      = errors.New("node already registered")
	ErrNoAvailableNode = errors.New("no available node on the cluster")
	ErrNoDiskSpace     = errors.New("no available disk space for this operation")
	ErrNotFound        = errors.New("cluster/node not found")
)
