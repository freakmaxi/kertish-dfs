package flags

import (
	"fmt"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strings"

	"github.com/freakmaxi/kertish-dfs/fs-tool/common"
	"github.com/freakmaxi/kertish-dfs/fs-tool/dfs"
	"github.com/freakmaxi/kertish-dfs/fs-tool/errors"
	"github.com/freakmaxi/kertish-dfs/fs-tool/terminal"
	"github.com/google/uuid"
)

type moveCommand struct {
	headAddresses []string
	output        terminal.Output
	basePath      string
	args          []string

	join      bool
	overwrite bool
	sources   []string
	target    string
}

func NewMove(headAddresses []string, output terminal.Output, basePath string, args []string) execution {
	return &moveCommand{
		headAddresses: headAddresses,
		output:        output,
		basePath:      basePath,
		args:          args,
	}
}

func (m *moveCommand) Parse() error {
	for len(m.args) > 0 {
		arg := m.args[0]
		switch arg {
		case "-f":
			m.args = m.args[1:]
			m.overwrite = true
			continue
		case "-j":
			m.args = m.args[1:]
			m.join = true
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

	if len(m.args) < 2 {
		return fmt.Errorf("mv command needs source and target parameters")
	}

	if !m.join && len(m.args) > 2 {
		return fmt.Errorf("mv command needs join flag to combine sources to target")
	}

	m.sources = m.args[:len(m.args)-1]
	m.target = m.args[len(m.args)-1]

	return nil
}

func (m *moveCommand) PrintUsage() {
	m.output.Println("  mv          Move file or folder.")
	m.output.Println("              Ex: mv [arguments] [source] [target]          # Move in dfs")
	m.output.Println("              Ex: mv [arguments] local:[source] [target]    # Move from local to dfs")
	m.output.Println("              Ex: mv [arguments] [source] local:[target]    # Move from dfs to local")
	m.output.Println("")
	m.output.Println("arguments:")
	m.output.Println("  -f          overwrites the existent file / folder")
	m.output.Println("  -j          joins sources to target file / folder")
	m.output.Println("")
	m.output.Refresh()
}

func (m *moveCommand) Name() string {
	return "mv"
}

func (m *moveCommand) Execute() error {
	onlyLocal := false
	for _, source := range m.sources {
		if strings.Index(source, local) == 0 {
			onlyLocal = true
			continue
		}
		if onlyLocal {
			return fmt.Errorf("mv command doesn't support to combine local and remote sources together to target")
		}
	}

	if onlyLocal {
		if err := m.localToRemote(); err != nil {
			return err
		}
		return nil
	}

	if strings.Index(m.target, local) == 0 {
		if err := m.remoteToLocal(); err != nil {
			return err
		}
		return nil
	}

	anim := common.NewAnimation(m.output, "processing...")
	anim.Start()

	for i := range m.sources {
		if filepath.IsAbs(m.sources[i]) {
			continue
		}
		m.sources[i] = common.Join(m.basePath, m.sources[i])
	}

	if !filepath.IsAbs(m.target) {
		m.target = common.Join(m.basePath, m.target)
	}

	if err := dfs.Change(m.headAddresses, m.sources, m.target, m.overwrite, false); err != nil {
		anim.Cancel()
		return err
	}
	anim.Stop()
	return nil
}

func (m *moveCommand) remoteToLocal() error {
	m.target = m.target[len(local):]

	info, err := os.Stat(m.target)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("local file can't open")
	}

	if info != nil && info.IsDir() {
		if len(m.sources) > 1 {
			return fmt.Errorf("please specify a target file name to combine")
		}

		_, sourceFileName := path.Split(m.sources[0])
		m.target = path.Join(m.target, sourceFileName)

		info, err = os.Stat(m.target)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("local file can't open")
		}
	}

	if info != nil {
		if info.IsDir() {
			return fmt.Errorf("target %s is a path", m.target)
		}

		if !m.overwrite {
			m.output.Printf("File %s is already exists\n", m.target)
			m.output.Print("Do you want to overwrite? (y/N) ")
			m.output.Refresh()

			var out string
			if !m.output.Scan(&out) {
				m.output.Println("unable to get the answer")
				m.output.Refresh()
				return nil
			}

			switch strings.ToLower(out) {
			case "y", "yes":
			default:
				m.output.Println("")
				m.output.Refresh()
				return nil
			}
		}
	}

	anim := common.NewAnimation(m.output, "processing...")
	anim.Start()

	for i := range m.sources {
		if filepath.IsAbs(m.sources[i]) {
			continue
		}
		m.sources[i] = common.Join(m.basePath, m.sources[i])
	}

	if strings.Index(m.target, "~") == 0 {
		u, err := user.Current()
		if err == nil {
			m.target = path.Join(u.HomeDir, m.target[1:])
		}
	}

	if err := dfs.Pull(m.headAddresses, m.sources, m.target, nil); err != nil {
		anim.Cancel()
		return err
	}

	for _, source := range m.sources {
		if err := dfs.Delete(m.headAddresses, source, false); err != nil {
			anim.Cancel()
			return err
		}
	}
	anim.Stop()
	return nil
}

func (m *moveCommand) localToRemote() error {
	if strings.Index(m.target, local) == 0 {
		return fmt.Errorf("please use O/S native commands to copy/move files/folders between local locations")
	}

	for i := range m.sources {
		m.sources[i] = m.sources[i][len(local):]
		if len(m.sources[i]) == 0 {
			return fmt.Errorf("please specify the source")
		}
		if strings.Index(m.sources[i], "~") == 0 {
			u, err := user.Current()
			if err == nil {
				m.sources[i] = path.Join(u.HomeDir, m.sources[i][1:])
			}
		}
	}

	anim := common.NewAnimation(m.output, "processing...")
	anim.Start()

	sourceTemp := m.sources[0]
	if len(m.sources) > 1 {
		sourceTemp := path.Join(os.TempDir(), uuid.New().String())
		defer os.Remove(sourceTemp)
		if err := createTemporary(m.sources, sourceTemp); err != nil {
			anim.Cancel()
			return err
		}
	}

	if !filepath.IsAbs(m.target) {
		m.target = common.Join(m.basePath, m.target)
	}

	if err := dfs.Put(m.headAddresses, sourceTemp, m.target, m.overwrite); err != nil {
		anim.Cancel()
		return fmt.Errorf(err.Error())
	}

	for _, source := range m.sources {
		if err := os.RemoveAll(source); err != nil {
			anim.Cancel()
			return fmt.Errorf("local file/folder couldn't delete")
		}
	}
	anim.Stop()
	return nil
}
