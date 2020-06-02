package flags

import (
	"fmt"
	"io"
	"os"

	"github.com/freakmaxi/kertish-dfs/basics/terminal"
)

type execution interface {
	Parse() error
	PrintUsage()

	Name() string
	Execute() error
}

func newExecution(headAddresses []string, output terminal.Output, command string, basePath string, args []string, version string) (execution, error) {
	switch command {
	case "ls":
		return NewList(headAddresses, output, basePath, args), nil
	case "mkdir":
		return NewMakeDirectory(headAddresses, output, basePath, args), nil
	case "cp":
		return NewCopy(headAddresses, output, basePath, args), nil
	case "mv":
		return NewMove(headAddresses, output, basePath, args), nil
	case "rm":
		return NewRemove(headAddresses, output, basePath, args), nil
	case "sh":
		return NewShell(headAddresses, version), nil
	}

	return nil, fmt.Errorf("unsupported command")
}

func cleanEmptyArguments(args []string) {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if len(arg) > 0 {
			continue
		}

		args = append(args[:i], args[i+1:]...)
		i--
	}
}

func createTemporary(sources []string, target string) error {
	targetFile, err := os.OpenFile(target, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("unable to create temporary file: %s", err.Error())
	}
	defer targetFile.Close()

	for _, source := range sources {
		info, err := os.Stat(source)
		if err != nil {
			return fmt.Errorf("unable to read %s", source)
		}
		if info.IsDir() {
			return fmt.Errorf("%s should be file", source)
		}

		if err := combine(source, targetFile); err != nil {
			return err
		}
	}

	return nil
}

func combine(source string, writer io.Writer) error {
	file, err := os.OpenFile(source, os.O_RDONLY, 0666)
	if err != nil {
		return fmt.Errorf("problem on accessing to file: %s", err.Error())
	}
	defer file.Close()

	if _, err := io.Copy(writer, file); err != nil {
		return fmt.Errorf("problem on combining %s to temporary file: %s", source, err.Error())
	}

	return nil
}
