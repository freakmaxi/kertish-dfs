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

type treeCommand struct {
	headAddresses []string
	output        terminal.Output
	basePath      string
	args          []string

	level        uint8
	indentations bool
	usage        bool
	source       string
}

// NewTree creates the execution of tree operation
func NewTree(headAddresses []string, output terminal.Output, basePath string, args []string) Execution {
	return &treeCommand{
		headAddresses: headAddresses,
		output:        output,
		basePath:      basePath,
		args:          args,
	}
}

func (t *treeCommand) Parse() error {
	t.level = 255
	t.indentations = true

	for len(t.args) > 0 {
		arg := t.args[0]
		switch arg {
		case "-L":
			t.args = t.args[1:]
			if len(t.args) == 0 {
				return fmt.Errorf("level argument needs value")
			}
			level, err := strconv.ParseUint(t.args[0], 10, 8)
			if err != nil {
				return err
			}
			t.args = t.args[1:]
			t.level = uint8(level)
			continue
		case "-i":
			t.args = t.args[1:]
			t.indentations = false
			continue
		case "-s":
			t.args = t.args[1:]
			t.usage = true
			continue
		case "-h":
			return errors.ErrShowUsage
		default:
			if strings.Index(arg, "-") == 0 {
				return fmt.Errorf("unsupported argument for tree command")
			}
		}
		break
	}

	t.args = sourceTargetArguments(t.args)
	t.args = cleanEmptyArguments(t.args)

	t.source = t.basePath
	if len(t.args) > 0 {
		if !filepath.IsAbs(t.args[0]) {
			t.source = path.Join(t.basePath, t.args[0])
		} else {
			t.source = t.args[0]
		}
	}

	return nil
}

func (t *treeCommand) PrintUsage() {
	t.output.Println("  tree        List folders as tree.")
	t.output.Println("              Ex: tree [arguments] [target]")
	t.output.Println("")
	t.output.Println("arguments:")
	t.output.Println("  -L level    defines the depth of level")
	t.output.Println("  -i          does not print indentation lines")
	t.output.Println("  -s          prints the size of each folder")
	t.output.Println("")
	t.output.Refresh()
}

func (t *treeCommand) Name() string {
	return "tree"
}

func (t *treeCommand) Execute() error {
	if strings.Index(t.source, local) == 0 {
		return fmt.Errorf("please use O/S native commands to get the tree view of folders")
	}

	anim := common.NewAnimation(t.output, "processing...")
	anim.Start()

	tree, err := dfs.Tree(t.headAddresses, t.source, t.usage)
	if err != nil {
		anim.Cancel()
		return err
	}
	anim.Stop()

	if t.indentations {
		t.printWithIndentations(tree)
	} else {
		t.printWithoutIndentations(tree)
	}
	return nil
}

func (t *treeCommand) printWithIndentations(tree *common.TreeShadow) {
	t.output.Println(tree.Full)

	totalFolders := 0
	t.printWithIndentationsChildren(tree.Folders, 0, []bool{len(tree.Folders) == 0}, &totalFolders)
	t.output.Println("")
	if totalFolders > 1 {
		t.output.Printf("%d directories\n", totalFolders)
	} else {
		t.output.Printf("%d directory\n", totalFolders)
	}
	t.output.Refresh()
}

func (t *treeCommand) printWithIndentationsChildren(folders common.TreeShadows, level uint8, ends []bool, totalFolders *int) {
	if t.level <= level {
		return
	}
	*totalFolders += len(folders)

	for i, folder := range folders {
		for l := uint8(1); l < level+1; l++ {
			if ends[l] {
				t.output.Print("    ")
			} else {
				t.output.Print("│   ")
			}
		}

		if len(folders) != i+1 {
			if t.usage {
				t.output.Printf("├── [%7s]  %s\n", t.sizeToString(folder.Size), folder.Name)
			} else {
				t.output.Printf("├── %s\n", folder.Name)
			}
			t.printWithIndentationsChildren(folder.Folders, level+1, append(ends, false), totalFolders)
			continue
		}
		if t.usage {
			t.output.Printf("└── [%7s]  %s\n", t.sizeToString(folder.Size), folder.Name)
		} else {
			t.output.Printf("└── %s\n", folder.Name)
		}
		t.printWithIndentationsChildren(folder.Folders, level+1, append(ends, true), totalFolders)
	}
}

func (t *treeCommand) printWithoutIndentations(tree *common.TreeShadow) {
	t.output.Println(tree.Full)

	totalFolders := 0
	t.printWithoutIndentationsChildren(tree.Folders, 0, &totalFolders)
	t.output.Println("")
	if totalFolders > 1 {
		t.output.Printf("%d directories\n", totalFolders)
	} else {
		t.output.Printf("%d directory\n", totalFolders)
	}
	t.output.Refresh()
}

func (t *treeCommand) printWithoutIndentationsChildren(folders common.TreeShadows, level uint8, totalFolders *int) {
	if t.level <= level {
		return
	}
	*totalFolders += len(folders)

	for i, folder := range folders {
		if len(folders) != i+1 {
			if t.usage {
				t.output.Printf("[%7s]  %s\n", t.sizeToString(folder.Size), folder.Name)
			} else {
				t.output.Printf("%s\n", folder.Name)
			}
			t.printWithoutIndentationsChildren(folder.Folders, level+1, totalFolders)
			continue
		}
		if t.usage {
			t.output.Printf("[%7s]  %s\n", t.sizeToString(folder.Size), folder.Name)
		} else {
			t.output.Printf("%s\n", folder.Name)
		}
		t.printWithoutIndentationsChildren(folder.Folders, level+1, totalFolders)
	}
}

func (t *treeCommand) sizeToString(size uint64) string {
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

var _ Execution = &treeCommand{}
