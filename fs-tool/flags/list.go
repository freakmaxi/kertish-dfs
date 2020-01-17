package flags

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/freakmaxi/2020-dfs/fs-tool/common"
	"github.com/freakmaxi/2020-dfs/fs-tool/dfs"
	"github.com/freakmaxi/2020-dfs/fs-tool/errors"
)

type listCommand struct {
	headAddresses []string
	args          []string

	listing bool
	usage   bool
	source  string
}

func NewList(headAddresses []string, args []string) execution {
	return &listCommand{
		headAddresses: headAddresses,
		args:          args,
	}
}

func (l *listCommand) Parse() error {
	for len(l.args) > 0 {
		arg := l.args[0]
		switch arg {
		case "-l":
			l.args = l.args[1:]
			l.listing = true
			continue
		case "-u":
			l.args = l.args[1:]
			l.usage = true
			continue
		case "-h":
			return errors.ErrShowUsage
		default:
			if strings.Index(arg, "-") == 0 {
				return fmt.Errorf("unsupported argument for ls command")
			}
		}
		break
	}

	if len(l.args) > 1 {
		return fmt.Errorf("ls command needs optionally source parameter")
	}

	l.source = "/"
	if len(l.args) == 1 {
		l.source = l.args[0]
	}

	return nil
}

func (l *listCommand) PrintUsage() {
	fmt.Println("  ls          List files and folders.")
	fmt.Println("              Ex: ls [arguments] [target]")
	fmt.Println()
	fmt.Println("arguments:")
	fmt.Println("  -l          shows in a listing format")
	fmt.Println("  -u          calculate the size of folders")
	fmt.Println()
}

func (l *listCommand) Execute() error {
	if strings.Index(l.source, local) == 0 {
		return fmt.Errorf("please use O/S native commands to list files/folders")
	}

	anim := common.NewAnimation("processing...")
	anim.Start()

	folder, err := dfs.List(l.headAddresses, l.source, l.usage)
	if err != nil {
		anim.Cancel()
		return err
	}
	anim.Stop()

	if l.listing {
		l.printAsList(folder)
	} else {
		l.printAsSummary(folder)
	}
	return nil
}

func (l *listCommand) printAsSummary(folder *common.Folder) {
	for _, f := range folder.Folders {
		if l.usage {
			fmt.Printf("> %s (%s)   ", f.Name, l.sizeToString(f.Size))
			continue
		}
		fmt.Printf("> %s   ", f.Name)
	}
	for _, f := range folder.Files {
		fmt.Printf("%s   ", f.Name)
	}
	fmt.Println()
}

func (l *listCommand) printAsList(folder *common.Folder) {
	total := len(folder.Folders) + len(folder.Files)

	if l.usage && total > 1 {
		fmt.Printf("total %d (%s)\n", total, l.sizeToString(folder.Size))
	} else {
		fmt.Printf("total %d\n", total)
	}

	for _, f := range folder.Folders {
		fmt.Printf("d %7v %s %s\n", l.sizeToString(f.Size), f.Created.Format("2006 Jan 02 03:04"), f.Name)
	}

	for _, f := range folder.Files {
		lockChar := "-"
		if f.Locked {
			lockChar = "•"
		}
		fmt.Printf("%s %7v %s %s\n", lockChar, l.sizeToString(f.Size), f.Modified.Format("2006 Jan 02 03:04"), f.Name)
	}
}

func (l *listCommand) sizeToString(size uint64) string {
	calculatedSize := size
	divideCount := 0
	for {
		calculatedSizeString := strconv.FormatUint(calculatedSize, 10)
		if len(calculatedSizeString) < 6 {
			break
		}
		calculatedSize /= 1024
		divideCount++
	}

	switch divideCount {
	case 0:
		return fmt.Sprintf("%sb", strconv.FormatUint(calculatedSize, 10))
	case 1:
		return fmt.Sprintf("%skb", strconv.FormatUint(calculatedSize, 10))
	case 2:
		return fmt.Sprintf("%smb", strconv.FormatUint(calculatedSize, 10))
	case 3:
		return fmt.Sprintf("%sgb", strconv.FormatUint(calculatedSize, 10))
	case 4:
		return fmt.Sprintf("%stb", strconv.FormatUint(calculatedSize, 10))
	}

	return "N/A"
}
