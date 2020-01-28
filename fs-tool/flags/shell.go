package flags

import (
	"fmt"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strings"

	"github.com/freakmaxi/kertish-dfs/fs-tool/common"
	"github.com/freakmaxi/kertish-dfs/fs-tool/errors"
	"github.com/freakmaxi/kertish-dfs/fs-tool/terminal"
	"github.com/gdamore/tcell"
	"github.com/mattn/go-runewidth"
)

const rootPath = "/"

type shellCommand struct {
	headAddresses []string
	version       string

	screen tcell.Screen
	output terminal.Output

	history []string
	buffer  string

	activeFolder *common.Folder
	foldersCache map[string]*common.Folder
}

func NewShell(headAddresses []string, version string) execution {
	return &shellCommand{
		headAddresses: headAddresses,
		version:       version,
		history:       make([]string, 0),
		buffer:        "",
		foldersCache:  make(map[string]*common.Folder),
	}
}

func (s *shellCommand) Parse() error {
	return nil
}

func (s *shellCommand) PrintUsage() {
	fmt.Println("  sh          Enter shell mode of fs-tool.")
	fmt.Println("              Ex: sh")
	fmt.Println()
}

func (s *shellCommand) initScreen() error {
	var err error
	s.screen, err = tcell.NewScreen()
	if err != nil {
		return err
	}

	if err := s.screen.Init(); err != nil {
		return err
	}

	s.output = terminal.NewTCellOut(s.screen)
	s.screen.Show()

	return nil
}

func (s *shellCommand) printHelp() {
	s.output.Println("available commands:")
	s.output.Println("  cd      Change directory.")
	s.output.Println("  mkdir   Create folders.")
	s.output.Println("  ls      List files and folders.")
	s.output.Println("  cp      Copy file or folder.")
	s.output.Println("  mv      Move file or folder.")
	s.output.Println("  rm      Remove files and/or folders.")
	s.output.Println("  help    Show this screen.")
	s.output.Println("          Ex: help [command] or help shortcuts")
	s.output.Println("  exit    Exit from shell.")
	s.output.Refresh()
}

func (s *shellCommand) printShortcuts() {
	s.output.Println("available shortcuts:")
	s.output.Println("  Escape    :   Clear/Cancel line")
	s.output.Println("  Up        :   Older history")
	s.output.Println("  Down      :   Newer history")
	s.output.Println("  Home      :   Move cursor to line head")
	s.output.Println("  End       :   Move cursor to line end")
	s.output.Println("  PageUp    :   Scroll up")
	s.output.Println("  PageDown  :   Scroll down")
	s.output.Println("  Ctrl+T    :   Top of the terminal")
	s.output.Println("  Ctrl+B    :   Bottom of the terminal")
	s.output.Println("  Ctrl+Y    :   Page up in the terminal")
	s.output.Println("  Ctrl+V    :   Page down in the terminal")
	s.output.Println("  Ctrl+W    :   Remove previous word")
	s.output.Println("  Backspace :   Remove previous char")
	s.output.Println("  Left      :   Move cursor to previous char")
	s.output.Println("  Alt+Left  :   Jump to previous word")
	s.output.Println("  Right     :   Move cursor to next char")
	s.output.Println("  Alt+Right :   Jump to next word")
	s.output.Println("  Ctrl+R    :   Refresh terminal cache")
	s.output.Println("  Tab       :   Complete path")
	s.output.Println("  Enter     :   Execute command")
	s.output.Refresh()
}

func (s *shellCommand) printWelcome() {
	s.output.Println("      __ _  ____  ____  ____  __  ____  _  _       ____  ____  ____")
	s.output.Println("     (  / )(  __)(  _ \\(_  _)(  )/ ___)/ )( \\     (    \\(  __)/ ___)")
	s.output.Println("      )  (  ) _)  )   /  )(   )( \\___ \\) __ (      ) D ( ) _) \\___ \\")
	s.output.Println("     (__\\_)(____)(__\\_) (__) (__)(____/\\_)(_/     (____/(__)  (____/")
	s.output.Printf("FileSystem Shell v%s, Visit: https://github.com/freakmaxi/kertish-dfs\n", s.version)
	s.output.Refresh()
}

func (s *shellCommand) Name() string {
	return "sh"
}

func (s *shellCommand) Execute() error {
	if err := s.initScreen(); err != nil {
		return err
	}
	defer s.screen.Fini()

	s.printWelcome()
	s.activeFolder =
		s.queryFolder(rootPath)
	if s.activeFolder == nil {
		return fmt.Errorf("communication error with dfs head")
	}
	s.readyLine()

	historyBack := -1
	for {
		ev := s.screen.PollEvent()
		switch ev := ev.(type) {
		case *tcell.EventKey:
			switch ev.Modifiers() {
			case tcell.ModAlt:
				name := ev.Name()

				if strings.Compare(name, "Alt+Rune[b]") == 0 {
					s.output.MoveCursorLeftWord()
					continue
				}

				if strings.Compare(name, "Alt+Rune[f]") == 0 {
					s.output.MoveCursorRightWord()
					continue
				}
			case tcell.ModCtrl:
				name := ev.Name()

				if strings.Compare(name, "Ctrl+T") == 0 {
					s.output.ScrollTop()
					continue
				}

				if strings.Compare(name, "Ctrl+Y") == 0 {
					s.output.ScrollPageUp()
					continue
				}

				if strings.Compare(name, "Ctrl+V") == 0 {
					s.output.ScrollPageDown()
					continue
				}

				if strings.Compare(name, "Ctrl+B") == 0 {
					s.output.ScrollBottom()
					continue
				}

				if strings.Compare(name, "Ctrl+W") == 0 {
					activeLine := s.output.ActiveLine()

					bufferParts := strings.Split(activeLine, " ")
					bpLength := len(bufferParts)

					if bpLength == 1 {
						lineLength :=
							runewidth.StringWidth(activeLine)
						s.output.Remove(lineLength)
						s.output.Refresh()

						s.buffer = s.output.ActiveLine()

						continue
					}

					lastPart := bufferParts[bpLength-1]
					lpLength := len(lastPart)
					if lpLength == 0 {
						lpLength = 1
					}
					s.output.Remove(lpLength)
					s.output.Refresh()

					s.buffer = s.output.ActiveLine()

					continue
				}

				if strings.Compare(name, "Ctrl+R") == 0 {
					s.rebuildActiveFolderAndCaches()
					s.readyLine()

					continue
				}
			default:
				switch ev.Key() {
				case tcell.KeyEscape:
					activeLine := s.output.ActiveLine()

					s.output.Remove(runewidth.StringWidth(activeLine))
					s.output.Refresh()

					s.buffer = s.output.ActiveLine()
				case tcell.KeyUp:
					s.printHistory(&historyBack, 1)
				case tcell.KeyDown:
					s.printHistory(&historyBack, -1)
				case tcell.KeyLeft:
					s.output.MoveCursorLeft(1)
				case tcell.KeyRight:
					s.output.MoveCursorRight(1)
				case tcell.KeyHome:
					s.output.MoveCursorHead()
				case tcell.KeyEnd:
					s.output.MoveCursorEnd()
				case tcell.KeyPgUp:
					s.output.ScrollUp()
				case tcell.KeyPgDn:
					s.output.ScrollDown()
				case tcell.KeyEnter:
					historyBack = -1

					if !s.processCommand() {
						return nil
					}
				case tcell.KeyBackspace2:
					s.handleBackspace()
				case tcell.KeyTab:
					activeLine := s.output.ActiveLine()
					bufferParts := strings.Split(activeLine, " ")

					s.handleTab(strings.Compare(bufferParts[0], "cd") == 0)
				default:
					r := ev.Rune()

					s.output.Print(string(r))
					s.output.Refresh()

					s.buffer = s.output.ActiveLine()
				}
			}
		case *tcell.EventResize:
			s.output.Refresh()
		}
	}
}

func (s *shellCommand) readyLine() {
	s.output.Println("")
	s.output.Printf("(%s)\n", s.activeFolder.Full)
	s.output.Print(" ➜ ")
	s.output.LockOrigin()
	s.output.Print(s.buffer)
	s.output.Refresh()
}

func (s *shellCommand) printHistory(historyBack *int, direction int) {
	if len(s.history) == 0 {
		return
	}

	if direction == 1 {
		*historyBack++
		if len(s.history) <= *historyBack {
			*historyBack = len(s.history) - 1
			return
		}
	} else if direction == -1 {
		*historyBack--
		if 0 > *historyBack {
			*historyBack = -1
			prev := s.history[*historyBack+1]
			s.output.Remove(len(prev))
			s.output.Refresh()

			s.buffer = s.output.ActiveLine()
			return
		}
	} else {
		return
	}

	lineLength := runewidth.StringWidth(s.buffer)
	s.output.Remove(lineLength)

	now := s.history[*historyBack]
	s.output.Print(now)
	s.output.Refresh()

	s.buffer = s.output.ActiveLine()
}

func (s *shellCommand) processCommand() bool {
	defer func() {
		s.readyLine()
	}()
	s.output.Println("")

	if len(s.buffer) == 0 {
		return true
	}

	args := strings.Split(s.buffer, " ")

	success, exit, e := s.parse(args)
	if !success {
		s.output.Printf("command not found: %s\n", args[0])
		s.output.Refresh()
		s.buffer = ""

		return true
	}

	if exit {
		return false
	}

	s.history = append([]string{s.buffer}, s.history...)
	s.buffer = ""

	if e != nil {
		if err := e.Execute(); err != nil {
			s.output.Println(err.Error())
		} else {
			switch e.Name() {
			case "cp", "mkdir", "mv", "rm":
				s.rebuildActiveFolderAndCaches()
			}
		}
	}

	return true
}

func (s *shellCommand) parse(args []string) (bool, bool, execution) {
	if len(args) == 0 {
		return true, false, nil
	}

	switch args[0] {
	case "cd":
		if len(args) < 2 {
			s.output.Println(s.activeFolder.Full)
			return true, false, nil
		}

		target := args[1]

		if strings.Index(target, local) == 0 {
			s.output.Println("cd command can only be used for dfs folders")
			return true, false, nil
		}

		target = common.Absolute(s.activeFolder.Full, target)

		if v, has := s.foldersCache[target]; has {
			s.activeFolder = v
			return true, false, nil
		}

		command := NewChangeDirectory(s.headAddresses, s.output, []string{target})
		if err := command.Parse(); err != nil {
			s.output.Println(err.Error())
			return true, false, nil
		}
		if err := command.Execute(); err != nil {
			s.output.Println(err.Error())
			return true, false, nil
		}

		s.activeFolder = command.(*changeDirectoryCommand).CurrentFolder
		return true, false, nil
	case "help":
		if len(args) < 2 {
			s.printHelp()
			return true, false, nil
		}

		if strings.Compare(args[1], "shortcuts") == 0 {
			s.output.Println("")
			s.printShortcuts()
			return true, false, nil
		}

		if strings.Compare(args[1], "cd") == 0 {
			command := NewChangeDirectory(s.headAddresses, s.output, nil)
			s.output.Println("")
			s.output.Println("Usage:")
			command.PrintUsage()

			return true, false, nil
		}

		var err error
		command, err := newExecution(s.headAddresses, s.output, args[1], s.activeFolder.Full, nil, s.version)
		if err != nil {
			s.output.Println(err.Error())
			return true, false, nil
		}
		s.output.Println("")
		s.output.Println("Usage:")
		command.PrintUsage()

		return true, false, nil
	case "exit":
		return true, true, nil
	case "mkdir", "ls", "cp", "mv", "rm":
		mrArgs := make([]string, 0)
		if len(args) > 1 {
			mrArgs = args[1:]
		}

		var err error
		command, err := newExecution(s.headAddresses, s.output, args[0], s.activeFolder.Full, mrArgs, s.version)
		if err != nil {
			s.output.Println(err.Error())
			return true, false, nil
		}

		err = command.Parse()
		if err != nil {
			if err != errors.ErrShowUsage {
				s.output.Println(err.Error())
			}
			s.output.Println("")
			s.output.Println("Usage:")
			command.PrintUsage()
			return true, false, nil
		}

		return true, false, command
	}

	return false, false, nil
}

func (s *shellCommand) rebuildActiveFolderAndCaches() {
	s.output.Println("")
	s.output.Println("rebuilding file/folder index...")
	p := rootPath
	if s.activeFolder != nil {
		p = s.activeFolder.Full
	}
	s.activeFolder =
		s.queryFolder(p)
	if s.activeFolder == nil {
		if strings.Compare(p, rootPath) == 0 {
			panic("communication error with dfs head")
		}
		s.rebuildActiveFolderAndCaches()
	}
	s.foldersCache = make(map[string]*common.Folder)

	s.output.Refresh()
}

func (s *shellCommand) queryFolder(folderPath string) *common.Folder {
	command := NewChangeDirectory(s.headAddresses, s.output, []string{folderPath})
	if err := command.Parse(); err != nil {
		s.output.Println(err.Error())
		return nil
	}
	if err := command.Execute(); err != nil {
		s.output.Println(err.Error())
		return nil
	}
	return command.(*changeDirectoryCommand).CurrentFolder
}

func (s *shellCommand) handleBackspace() {
	s.output.Remove(1)
	s.output.Refresh()

	s.buffer = s.output.ActiveLine()
}

func (s *shellCommand) handleTab(cdRequest bool) {
	bufParts := strings.Split(s.buffer, " ")
	if len(bufParts) == 1 {
		return
	}
	lastPart := bufParts[len(bufParts)-1]

	localSearch := false
	if strings.Index(lastPart, local) == 0 {
		localSearch = true
		lastPart = lastPart[len(local):]
	}

	parent, searching := path.Split(lastPart)

	if localSearch {
		if strings.Index(parent, string(os.PathSeparator)) != 0 {
			if len(parent) > 0 && parent[0] == '~' {
				u, err := user.Current()
				if err == nil {
					parent = path.Join(u.HomeDir, parent[1:])
				}
			} else {
				pwd, err := os.Getwd()
				if err == nil {
					parent = path.Join(pwd, parent)
				}
			}
			parent, _ = filepath.Abs(parent)
		}
	} else {
		parent = common.Absolute(s.activeFolder.Full, parent)
	}

	v := s.searchInFolder(parent, searching, localSearch, cdRequest)
	if len(v) > 0 {
		s.output.Print(v)
		s.output.Refresh()

		s.buffer = s.output.ActiveLine()
	}
}

func (s *shellCommand) searchInFolder(basePath string, v string, local bool, cdRequest bool) string {
	if local {
		return s.searchInLocalFolder(basePath, v, cdRequest)
	}
	return s.searchInDfsFolder(basePath, v, cdRequest)
}

func (s *shellCommand) searchInDfsFolder(basePath string, v string, onlyFolders bool) string {
	printSummaryFunc := func(matches [][]string) {
		s.output.Println("")
		for _, m := range matches {
			if strings.Compare(m[0], "folder") == 0 {
				s.output.Print("> ")
			}
			s.output.Printf("%s   ", m[1])
		}
		s.output.Println("")
		s.readyLine()
	}

	basePath = common.CorrectPath(basePath)

	workingFolder := s.activeFolder
	if strings.Compare(basePath, s.activeFolder.Full) != 0 {
		if v, has := s.foldersCache[basePath]; has {
			workingFolder = v
		} else {
			s.output.Println("")
			workingFolder = s.queryFolder(basePath)
			s.readyLine()
			if workingFolder == nil {
				return ""
			}
			s.foldersCache[workingFolder.Full] = workingFolder
		}
	}

	matches := make([][]string, 0)
	for _, folder := range workingFolder.Folders {
		if strings.Index(folder.Name, v) != 0 {
			continue
		}
		matches = append(matches, []string{"folder", folder.Name})
	}

	if !onlyFolders {
		for _, file := range workingFolder.Files {
			if strings.Index(file.Name, v) != 0 {
				continue
			}
			matches = append(matches, []string{"file", file.Name})
		}
	}

	if len(matches) == 0 {
		return ""
	}

	if len(matches) == 1 {
		return matches[0][1][len(v):]
	}

	match, matches := s.matchReduce(matches)

	printSummaryFunc(matches)
	return match[len(v):]
}

func (s *shellCommand) searchInLocalFolder(basePath string, v string, onlyFolders bool) string {
	printSummaryFunc := func(matches [][]string) {
		s.output.Println("")
		for _, m := range matches {
			if strings.Compare(m[0], "folder") == 0 {
				s.output.Print("> ")
			}
			s.output.Printf("%s   ", m[1])
		}
		s.output.Println("")
		s.readyLine()
	}

	if len(basePath) == 0 {
		basePath = string(os.PathSeparator)
	}

	if len(basePath) > 1 && basePath[len(basePath)-1] == os.PathSeparator {
		basePath = basePath[:len(basePath)-1]
	}

	_, basePathName := path.Split(basePath)
	matches := make([][]string, 0)
	if err := filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsPermission(err) {
				return nil
			}
			return err
		}
		if strings.Compare(path, basePath) == 0 || strings.Compare(info.Name(), basePathName) == 0 {
			return nil
		}
		if strings.Index(info.Name(), v) != 0 {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.IsDir() {
			matches = append(matches, []string{"folder", info.Name()})
			return filepath.SkipDir
		} else {
			if !onlyFolders {
				matches = append(matches, []string{"file", info.Name()})
			}
		}
		return nil
	}); err != nil {
		return ""
	}

	if len(matches) == 0 {
		return ""
	}

	if len(matches) == 1 {
		return matches[0][1][len(v):]
	}

	match, matches := s.matchReduce(matches)

	printSummaryFunc(matches)
	return match[len(v):]
}

func (s *shellCommand) matchReduce(matches [][]string) (string, [][]string) {
	if len(matches) == 0 {
		return matches[0][1], matches
	}

	if len(matches) == 1 {
		return "", matches
	}

	reduced := make([]rune, 0)
	index := 0
	for {
		for _, match := range matches {
			runeMatch := []rune(match[1])

			if len(runeMatch) == index {
				return string(reduced), matches
			}

			if index == len(reduced) {
				reduced = append(reduced, runeMatch[index])
				continue
			}

			if reduced[index] != runeMatch[index] {
				return string(reduced[:index]), matches
			}
		}
		index++
	}
}
