package flags

import (
	"fmt"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/terminal"
	"github.com/freakmaxi/kertish-dfs/fs-tool/dfs"
)

type changeDirectoryCommand struct {
	headAddresses []string
	output        terminal.Output
	args          []string

	target string

	CurrentFolder *common.Folder
}

func NewChangeDirectory(headAddresses []string, output terminal.Output, args []string) execution {
	return &changeDirectoryCommand{
		headAddresses: headAddresses,
		output:        output,
		args:          args,
	}
}

func (c *changeDirectoryCommand) Parse() error {
	if len(c.args) != 1 {
		return fmt.Errorf("cd command needs only target parameter")
	}

	c.target = c.args[0]

	return nil
}

func (c *changeDirectoryCommand) PrintUsage() {
	c.output.Println("  cd          Change folders.")
	c.output.Println("              Ex: cd [target]")
	c.output.Println("")
	c.output.Refresh()
}

func (c *changeDirectoryCommand) Name() string {
	return "cd"
}

func (c *changeDirectoryCommand) Execute() error {
	if strings.Index(c.target, local) == 0 {
		return fmt.Errorf("cd works only for dfs folder(s)")
	}

	anim := common.NewAnimation(c.output, "processing...")
	anim.Start()

	folder, err := dfs.List(c.headAddresses, c.target, false)
	if err != nil {
		anim.Cancel()
		return err
	}
	anim.Stop()

	c.CurrentFolder = folder

	return nil
}
