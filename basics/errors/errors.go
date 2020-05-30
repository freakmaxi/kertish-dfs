package errors

import "errors"

var (
	ErrQuit = errors.New("operation does not need to continue")

	ErrNoSpace               = errors.New("no space on clusters")
	ErrNoAvailableActionNode = errors.New("no available node for clusters")
	ErrCreate                = errors.New("not possible to create shadow on data node")
	ErrLock                  = errors.New("path/file is locked")
	ErrRepair                = errors.New("inconsistency detected, require repairing")
	ErrZombie                = errors.New("path/file is zombie")
	ErrZombieAlive           = errors.New("zombie file is still alive, try again to kill")
	ErrJoinConflict          = errors.New("joining source folders will have conflict in target")
	ErrSync                  = errors.New("syncing is failed")

	ErrExists                       = errors.New("cluster is already exists")
	ErrPing                         = errors.New("node is not reachable")
	ErrJoin                         = errors.New("node joining to cluster is failed")
	ErrMode                         = errors.New("setting node mode is failed")
	ErrLastNode                     = errors.New("last node of the cluster can not be removed")
	ErrRegistered                   = errors.New("node already registered")
	ErrNoAvailableClusterNode       = errors.New("no available node on the cluster")
	ErrNotAvailableForClusterAction = errors.New("cluster is not available for cluster wide actions")
	ErrNoDiskSpace                  = errors.New("no available disk space for this operation")
	ErrNotFound                     = errors.New("cluster/node not found")

	ErrShowUsage  = errors.New("show usage")
	ErrProcessing = errors.New("another operation in progress")
)
