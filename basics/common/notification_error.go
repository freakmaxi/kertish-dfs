package common

import "errors"

type NotificationError struct {
	containerList NotificationContainerList
	err           error
}

func NewNotificationError(containerList NotificationContainerList, err error) error {
	return &NotificationError{
		containerList: containerList,
		err:           err,
	}
}

func (n *NotificationError) ContainerList() NotificationContainerList {
	return n.containerList
}

func (n *NotificationError) Is(err error) bool {
	return errors.Is(n.err, err)
}

func (n *NotificationError) Error() string {
	return n.err.Error()
}

var _ error = &NotificationError{}
