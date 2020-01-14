package flags

import (
	"fmt"
	"strings"

	"github.com/freakmaxi/2020-dfs/fs-tool/common"
	"github.com/freakmaxi/2020-dfs/fs-tool/dfs"
)

type makeDirectoryCommand struct {
	headAddresses []string
	args          []string

	target string
}

func NewMakeDirectory(headAddresses []string, args []string) execution {
	return &makeDirectoryCommand{
		headAddresses: headAddresses,
		args:          args,
	}
}

func (m *makeDirectoryCommand) Parse() error {
	if len(m.args) != 1 {
		return fmt.Errorf("mkdir command needs only target parameter")
	}

	m.target = m.args[0]

	return nil
}

func (m *makeDirectoryCommand) PrintUsage() {
	fmt.Println("  mkdir       Create folders.")
	fmt.Println("              Ex: mkdir [target]")
	fmt.Println()
}

func (m *makeDirectoryCommand) Execute() error {
	if strings.Index(m.target, local) == 0 {
		return fmt.Errorf("please use O/S native commands to create folder(s)")
	}

	anim := common.NewAnimation("processing...")
	anim.Start()

	if err := dfs.MakeFolder(m.headAddresses, m.target); err != nil {
		anim.Cancel()
		return err
	}
	anim.Stop()
	return nil
}
