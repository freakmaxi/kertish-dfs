package flags

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/freakmaxi/kertish-dfs/fs-tool/common"
	"github.com/freakmaxi/kertish-dfs/fs-tool/dfs"
	"github.com/freakmaxi/kertish-dfs/fs-tool/errors"
	"github.com/freakmaxi/kertish-dfs/fs-tool/terminal"
	"github.com/google/uuid"
)

type copyCommand struct {
	headAddresses []string
	output        terminal.Output
	basePath      string
	args          []string

	join      bool
	overwrite bool
	readRange *common.ReadRange
	sources   []string
	target    string
}

func NewCopy(headAddresses []string, output terminal.Output, basePath string, args []string) execution {
	return &copyCommand{
		headAddresses: headAddresses,
		output:        output,
		basePath:      basePath,
		args:          args,
	}
}

func (c *copyCommand) Parse() error {
	for len(c.args) > 0 {
		arg := c.args[0]
		switch arg {
		case "-f":
			c.args = c.args[1:]
			c.overwrite = true
			continue
		case "-j":
			c.args = c.args[1:]
			c.join = true
			continue
		case "-r":
			c.args = c.args[1:]
			if len(c.args) == 0 {
				return fmt.Errorf("range argument needs value")
			}
			readRange, err := common.NewReadRange(c.args[0])
			if err != nil {
				return err
			}
			c.args = c.args[1:]
			c.readRange = readRange
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

	if len(c.args) < 2 {
		return fmt.Errorf("cp command needs source and target parameters")
	}

	if !c.join && len(c.args) > 2 {
		return fmt.Errorf("cp command needs join flag to combine sources to target")
	}

	c.sources = c.args[:len(c.args)-1]
	c.target = c.args[len(c.args)-1]

	return nil
}

func (c *copyCommand) PrintUsage() {
	c.output.Println("  cp          Copy file or folder.")
	c.output.Println("              Ex: cp [arguments] [source] [target]          # Copy in dfs")
	c.output.Println("              Ex: cp [arguments] local:[source] [target]    # Copy from local to dfs")
	c.output.Println("              Ex: cp [arguments] [source] local:[target]    # Copy from dfs to local")
	c.output.Println("")
	c.output.Println("arguments:")
	c.output.Println("  -f          overwrites the existent file / folder")
	c.output.Println("  -j          joins sources to target file / folder")
	c.output.Println("  -r value    copies only defined range of the file.")
	c.output.Println("              Ex: cp -r [byteBegins]->[byteEnds] [source] local:[target]")
	c.output.Println("")
	c.output.Println("              WARNING: range works only from dfs to local copy operations")
	c.output.Println("")
	c.output.Refresh()
}

func (c *copyCommand) Name() string {
	return "cp"
}

func (c *copyCommand) Execute() error {
	onlyLocal := false
	for _, source := range c.sources {
		if strings.Index(source, local) == 0 {
			onlyLocal = true
			continue
		}
		if onlyLocal {
			return fmt.Errorf("cp command doesn't support to combine local and remote sources together to target")
		}
	}

	if onlyLocal {
		if err := c.localToRemote(); err != nil {
			return err
		}
		return nil
	}

	if strings.Index(c.target, local) == 0 {
		if err := c.remoteToLocal(); err != nil {
			return err
		}
		return nil
	}

	anim := common.NewAnimation(c.output, "processing...")
	anim.Start()

	for i := range c.sources {
		if filepath.IsAbs(c.sources[i]) {
			continue
		}
		c.sources[i] = common.Join(c.basePath, c.sources[i])
	}

	if !filepath.IsAbs(c.target) {
		c.target = common.Join(c.basePath, c.target)
	}

	if err := dfs.Change(c.headAddresses, c.sources, c.target, c.overwrite, true); err != nil {
		anim.Cancel()
		return err
	}
	anim.Stop()
	return nil
}

func (c *copyCommand) remoteToLocal() error {
	c.target = c.target[len(local):]

	info, err := os.Stat(c.target)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("local file can't open")
	}

	if info != nil && info.IsDir() {
		if len(c.sources) > 1 {
			return fmt.Errorf("please specify a target file name to combine")
		}

		_, sourceFileName := path.Split(c.sources[0])
		c.target = path.Join(c.target, sourceFileName)

		info, err = os.Stat(c.target)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("local file can't open")
		}
	}

	if info != nil {
		if info.IsDir() {
			return fmt.Errorf("target %s is a path", c.target)
		}

		if !c.overwrite {
			c.output.Printf("File %s is already exists\n", c.target)
			c.output.Print("Do you want to overwrite? (y/N) ")
			c.output.Refresh()

			var out string
			if !c.output.Scan(&out) {
				c.output.Println("unable to get the answer")
				c.output.Refresh()
				return nil
			}

			switch strings.ToLower(out) {
			case "y", "yes":
			default:
				c.output.Println("")
				c.output.Refresh()
				return nil
			}
		}
	}

	anim := common.NewAnimation(c.output, "processing...")
	anim.Start()

	for i := range c.sources {
		if filepath.IsAbs(c.sources[i]) {
			continue
		}
		c.sources[i] = common.Join(c.basePath, c.sources[i])
	}

	if err := dfs.Pull(c.headAddresses, c.sources, c.target, c.readRange); err != nil {
		anim.Cancel()
		return err
	}
	anim.Stop()
	return nil
}

func (c *copyCommand) localToRemote() error {
	if strings.Index(c.target, local) == 0 {
		return fmt.Errorf("please use O/S native commands to copy/move files/folders between local locations")
	}

	for i := range c.sources {
		c.sources[i] = c.sources[i][len(local):]
		if len(c.sources[i]) == 0 {
			return fmt.Errorf("please specify the source")
		}
	}

	anim := common.NewAnimation(c.output, "processing...")
	anim.Start()

	sourceTemp := path.Join(os.TempDir(), uuid.New().String())
	defer os.Remove(sourceTemp)
	if err := createTemporary(c.sources, sourceTemp); err != nil {
		anim.Cancel()
		return err
	}

	if !filepath.IsAbs(c.target) {
		c.target = common.Join(c.basePath, c.target)
	}

	if err := dfs.Put(c.headAddresses, sourceTemp, c.target, c.overwrite); err != nil {
		anim.Cancel()
		return fmt.Errorf(err.Error())
	}
	anim.Stop()
	return nil
}
