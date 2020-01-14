package flags

import "fmt"

type execution interface {
	Parse() error
	PrintUsage()

	Execute() error
}

func newExecution(headAddresses []string, command string, args []string) (execution, error) {
	switch command {
	case "ls":
		return NewList(headAddresses, args), nil
	case "mkdir":
		return NewMakeDirectory(headAddresses, args), nil
	case "cp":
		return NewCopy(headAddresses, args), nil
	case "mv":
		return NewMove(headAddresses, args), nil
	case "rm":
		return NewRemove(headAddresses, args), nil
	}

	return nil, fmt.Errorf("unsupported command")
}
