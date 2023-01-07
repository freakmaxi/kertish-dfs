package flags

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/terminal"
)

type Execution interface {
	Parse() error
	PrintUsage()

	Name() string
	Execute() error
}

var targetRegex, _ = regexp.Compile("(\"[\\w\\W\\d\\s]+\")")

func newExecution(headAddresses []string, output terminal.Output, command string, basePath string, args []string, version string) (Execution, error) {
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
	case "tree":
		return NewTree(headAddresses, output, basePath, args), nil
	case "sh":
		return NewShell(headAddresses, version), nil
	}

	return nil, fmt.Errorf("unsupported command")
}

func cleanEmptyArguments(args []string) []string {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if len(arg) > 0 {
			continue
		}

		args = append(args[:i], args[i+1:]...)
		i--
	}
	return args
}

func sourceTargetArguments(args []string) []string {
	output := make([]string, 0)
	argsMap := make(map[string]bool)

	addRemainFunc := func(joinedArgs string) {
		remainArgs := strings.Split(joinedArgs, " ")
		for len(remainArgs) > 0 {
			arg := remainArgs[0]
			if _, has := argsMap[arg]; !has {
				output = append(output, arg)
				argsMap[arg] = true
			}
			remainArgs = remainArgs[1:]
		}
	}

	joinedArgs := strings.Join(args, " ")
	lastIdx := 0

	idxes := targetRegex.FindAllStringIndex(joinedArgs, -1)
	for len(idxes) > 0 {
		idx := idxes[0]

		preJoinedArgs := joinedArgs[lastIdx:idx[0]]
		addRemainFunc(preJoinedArgs)

		arg := joinedArgs[idx[0]+1 : idx[1]-1]
		if _, has := argsMap[arg]; !has {
			output = append(output, arg)
			argsMap[arg] = true
		}

		lastIdx = idx[1]
		idxes = idxes[1:]
	}

	preJoinedArgs := joinedArgs[lastIdx:]
	addRemainFunc(preJoinedArgs)

	return output
}

func createTemporary(sources []string, target string) error {
	targetFile, err := os.OpenFile(target, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("unable to create temporary file: %s", err.Error())
	}
	defer func() { _ = targetFile.Close() }()

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
	defer func() { _ = file.Close() }()

	if _, err := io.Copy(writer, file); err != nil {
		return fmt.Errorf("problem on combining %s to temporary file: %s", source, err.Error())
	}

	return nil
}
