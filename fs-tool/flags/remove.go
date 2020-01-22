package flags

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/freakmaxi/kertish-dfs/fs-tool/common"
	"github.com/freakmaxi/kertish-dfs/fs-tool/dfs"
	"github.com/freakmaxi/kertish-dfs/fs-tool/errors"
)

type removeCommand struct {
	headAddresses []string
	args          []string

	confirm bool
	targets []string
}

func NewRemove(headAddresses []string, args []string) execution {
	return &removeCommand{
		headAddresses: headAddresses,
		args:          args,
		confirm:       true,
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
		case "-h":
			return errors.ErrShowUsage
		default:
			if strings.Index(arg, "-") == 0 {
				return fmt.Errorf("unsupported argument for cp command")
			}
		}
		break
	}

	if len(r.args) == 0 {
		return fmt.Errorf("rm command needs target parameters")
	}

	r.targets = make([]string, len(r.args))
	copy(r.targets, r.args)

	return nil
}

func (r *removeCommand) PrintUsage() {
	fmt.Println("  rm          Remove files and/or folders.")
	fmt.Println("              Ex: rm [arguments] [target] [target] [target] ...")
	fmt.Println()
	fmt.Println("arguments:")
	fmt.Println("  -f          skip confirmation and removes")
	fmt.Println()
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

		fmt.Println("You are about to remove")
		fmt.Print(list)
		fmt.Print("Do you want to continue? (y/N) ")

		reader := bufio.NewReader(os.Stdin)
		char, _, err := reader.ReadRune()
		if err != nil {
			fmt.Println(err)
		}

		switch char {
		case 'Y', 'y':
		default:
			return nil
		}
	}

	anim := common.NewAnimation("processing...")
	anim.Start()

	for _, d := range r.targets {
		if err := dfs.Delete(r.headAddresses, d); err != nil {
			anim.Cancel()
			return err
		}
	}
	anim.Stop()
	return nil
}
