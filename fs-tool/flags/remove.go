package flags

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/freakmaxi/kertish-dfs/basics/terminal"
	"github.com/freakmaxi/kertish-dfs/fs-tool/dfs"
)

type removeCommand struct {
	headAddresses []string
	output        terminal.Output
	basePath      string
	args          []string

	confirm     bool
	killZombies bool
	targets     []string
}

func NewRemove(headAddresses []string, output terminal.Output, basePath string, args []string) Execution {
	return &removeCommand{
		headAddresses: headAddresses,
		output:        output,
		basePath:      basePath,
		args:          args,
		confirm:       true,
		killZombies:   false,
		targets:       make([]string, 0),
	}
}

func (r *removeCommand) Parse() error {
	for len(r.args) > 0 {
		arg := r.args[0]
		switch arg {
		case "-f":
			r.args = r.args[1:]
			r.confirm = false
			continue
		case "-k":
			r.args = r.args[1:]
			r.killZombies = true
			continue
		case "-h":
			return errors.ErrShowUsage
		default:
			if strings.Index(arg, "-") == 0 {
				return fmt.Errorf("unsupported argument for cp command")
			}
		}
		break
	}

	r.targets = sourceTargetArguments(r.args)
	r.targets = cleanEmptyArguments(r.targets)

	if len(r.targets) == 0 {
		return fmt.Errorf("rm command needs target parameters")
	}

	return nil
}

func (r *removeCommand) PrintUsage() {
	r.output.Println("  rm          Remove files and/or folders.")
	r.output.Println("              Ex: rm [arguments] [target] [target] [target] ...")
	r.output.Println("")
	r.output.Println("arguments:")
	r.output.Println("  -f          skip confirmation and removes")
	r.output.Println("  -k          try to kill zombie file(s)")
	r.output.Println("")
	r.output.Refresh()
}

func (r *removeCommand) Name() string {
	return "rm"
}

func (r *removeCommand) Execute() error {
	if r.confirm {
		list := ""
		for _, d := range r.targets {
			if strings.Index(d, local) == 0 {
				return fmt.Errorf("please use O/S native commands to delete files/folders")
			}
			list = fmt.Sprintf("%s  - %s\n", list, d)
		}

		r.output.Println("You are about to remove")
		r.output.Print(list)
		r.output.Print("Do you want to continue? (y/N) ")
		r.output.Refresh()

		var out string
		if !r.output.Scan(&out) {
			r.output.Println("unable to get the answer")
			r.output.Refresh()
			return nil
		}

		switch strings.ToLower(out) {
		case "y", "yes":
		default:
			return nil
		}
	}

	anim := common.NewAnimation(r.output, "processing...")
	anim.Start()

	for _, d := range r.targets {
		if !filepath.IsAbs(d) {
			d = common.Join(r.basePath, d)
		}

		if err := dfs.Delete(r.headAddresses, d, r.killZombies); err != nil {
			anim.Cancel()
			return err
		}
	}
	anim.Stop()
	return nil
}

var _ Execution = &removeCommand{}
