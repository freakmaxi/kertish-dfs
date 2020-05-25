package flags

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/terminal"
	"github.com/freakmaxi/kertish-dfs/fs-tool/dfs"
)

type makeDirectoryCommand struct {
	headAddresses []string
	output        terminal.Output
	basePath      string
	args          []string

	target string
}

func NewMakeDirectory(headAddresses []string, output terminal.Output, basePath string, args []string) execution {
	return &makeDirectoryCommand{
		headAddresses: headAddresses,
		output:        output,
		basePath:      basePath,
		args:          args,
	}
}

func (m *makeDirectoryCommand) Parse() error {
	cleanEmptyArguments(m.args)

	if len(m.args) != 1 {
		return fmt.Errorf("mkdir command needs only target parameter")
	}

	m.target = m.args[0]

	return nil
}

func (m *makeDirectoryCommand) PrintUsage() {
	m.output.Println("  mkdir       Create folders.")
	m.output.Println("              Ex: mkdir [target]")
	m.output.Println("")
}

func (m *makeDirectoryCommand) Name() string {
	return "mkdir"
}

func (m *makeDirectoryCommand) Execute() error {
	if strings.Index(m.target, local) == 0 {
		return fmt.Errorf("please use O/S native commands to create folder(s)")
	}

	if !filepath.IsAbs(m.target) {
		m.target = common.Join(m.basePath, m.target)
	}

	anim := common.NewAnimation(m.output, "processing...")
	anim.Start()

	if err := dfs.MakeFolder(m.headAddresses, m.target); err != nil {
		anim.Cancel()
		return err
	}
	anim.Stop()
	return nil
}
