package flags

import (
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/freakmaxi/kertish-dfs/basics/terminal"
	"github.com/freakmaxi/kertish-dfs/fs-tool/dfs"
)

type listCommand struct {
	headAddresses []string
	output        terminal.Output
	basePath      string
	args          []string

	listing bool
	usage   bool
	source  string
}

func NewList(headAddresses []string, output terminal.Output, basePath string, args []string) Execution {
	return &listCommand{
		headAddresses: headAddresses,
		output:        output,
		basePath:      basePath,
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

	l.args = sourceTargetArguments(l.args)
	l.args = cleanEmptyArguments(l.args)

	l.source = l.basePath
	if len(l.args) > 0 {
		if !filepath.IsAbs(l.args[0]) {
			l.source = path.Join(l.basePath, l.args[0])
		} else {
			l.source = l.args[0]
		}
	}

	return nil
}

func (l *listCommand) PrintUsage() {
	l.output.Println("  ls          List files and folders.")
	l.output.Println("              Ex: ls [arguments] [target]")
	l.output.Println("")
	l.output.Println("arguments:")
	l.output.Println("  -l          shows in a listing format")
	l.output.Println("  -u          calculate the size of folders")
	l.output.Println("")
	l.output.Println("marking:")
	l.output.Println("  d           folder")
	l.output.Println("  -           file")
	l.output.Println("  •           locked")
	l.output.Println("  ↯           zombie")
	l.output.Println("")
	l.output.Refresh()
}

func (l *listCommand) Name() string {
	return "ls"
}

func (l *listCommand) Execute() error {
	if strings.Index(l.source, local) == 0 {
		return fmt.Errorf("please use O/S native commands to list files/folders")
	}

	anim := common.NewAnimation(l.output, "processing...")
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
			l.output.Printf("> %s (%s)   ", f.Name, l.sizeToString(f.Size))
			continue
		}
		l.output.Printf("> %s   ", f.Name)
	}
	for _, f := range folder.Files {
		l.output.Printf("%s   ", f.Name)
	}
	l.output.Println("")
	l.output.Refresh()
}

func (l *listCommand) printAsList(folder *common.Folder) {
	total := len(folder.Folders) + len(folder.Files)

	if l.usage && total > 1 {
		l.output.Printf("total %d (%s)\n", total, l.sizeToString(folder.Size))
	} else {
		l.output.Printf("total %d\n", total)
	}

	for _, f := range folder.Folders {
		l.output.Printf("d %7v %s %s\n", l.sizeToString(f.Size), f.Created.Format(common.FriendlyTimeFormat), f.Name)
	}

	for _, f := range folder.Files {
		name := f.Name
		fileChar := "-"
		if f.Locked() {
			fileChar = "•"
			name = fmt.Sprintf("%s (locked till %s)", name, f.Lock.Till.Local().Format(common.FriendlyTimeFormat))
		} else if f.ZombieCheck() {
			fileChar = "↯"
		}
		l.output.Printf("%s %7v %s %s\n", fileChar, l.sizeToString(f.Size), f.Modified.Local().Format(common.FriendlyTimeFormat), name)
	}

	l.output.Refresh()
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

var _ Execution = &listCommand{}
