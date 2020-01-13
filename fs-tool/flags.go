package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
)

type readRange struct {
	begins int64
	ends   int64
}

func NewReadRange() readRange {
	return readRange{
		begins: 0,
		ends:   -1,
	}
}

func (r *readRange) String() string {
	if r == nil {
		return ""
	}
	return fmt.Sprintf("%d-%d", r.begins, r.ends)
}

func (r *readRange) Set(value string) error {
	rr := strings.Split(value, "-")
	if len(rr) < 2 {
		rr = []string{"0", ""}
	}

	var err error
	r.begins, err = strconv.ParseInt(rr[0], 10, 64)
	if err != nil {
		return fmt.Errorf("range is not valid")
	}

	if r.begins < 0 {
		return fmt.Errorf("range is not valid")
	}

	r.ends, err = strconv.ParseInt(rr[1], 10, 64)
	if err != nil {
		r.ends = -1
	}

	if r.ends > -1 && r.ends < r.begins {
		return fmt.Errorf("range can not end before begins")
	}

	return nil
}

func (r *readRange) HasRange() bool {
	return r.begins > 0 && r.ends != -1
}

type flagContainer struct {
	headAddress string
	command     string
	params      []string

	//downloadFlag  bool
	overwriteFlag bool
	//rangeFlag     readRange
}

func (f *flagContainer) Validate() bool {
	switch f.command {
	case "ls", "mkdir", "rm":
		/*if f.rangeFlag.HasRange() {
			fmt.Printf("range option is not allowed to use with %s command\n", f.command)
			return false
		}*/

		if f.overwriteFlag {
			fmt.Printf("overwrite option is not allowed to use with %s command\n", f.command)
			return false
		}

		/*if f.downloadFlag {
			fmt.Printf("download option is not allowed to use with %s command\n", f.command)
			return false
		}*/
		/*case "mv":
		if f.rangeFlag.HasRange() {
			fmt.Printf("range option is not allowed to use with %s command\n", f.command)
			return false
		}

		if f.downloadFlag {
			fmt.Printf("download option is not allowed to use with %s command\n", f.command)
			return false
		}*/
	}
	return true
}

func printSupportedActions(set *flag.FlagSet) {
	_, filename := path.Split(os.Args[0])

	fmt.Println("2020-dfs usage: ")
	fmt.Println()
	fmt.Printf("   %s [options] command parameters\n", filename)
	fmt.Println()
	fmt.Println("options:")
	set.PrintDefaults()
	fmt.Println()
	fmt.Println("commands:")
	fmt.Println("  ls    List file and folder.")
	fmt.Println("           Ex: ls [SOURCE]")
	fmt.Println("  mkdir Delete file or folder.")
	fmt.Println("           Ex: mkdir [TARGET]")
	fmt.Println("  cp    Copy file or folder.")
	fmt.Println("           Ex: cp [SOURCE] [TARGET] or cp local:[SOURCE] [TARGET]")
	fmt.Println("  mv    Move file or folder.")
	fmt.Println("           Ex: mv [SOURCE] [TARGET] or mv [SOURCE] local:[TARGET]")
	fmt.Println("  rm    Delete file or folder.")
	fmt.Println("           Ex: rm [TARGET]")
	fmt.Println()
}

func grabAction(downloadFlag bool) (*string, []string) {
	argsLength := len(os.Args) - 1
	for i := argsLength; i > 0; i-- {
		arg := os.Args[i]

		switch arg {
		case "mv":
			if argsLength-i == 2 {
				return &arg, []string{os.Args[argsLength-1], os.Args[argsLength]}
			}
			fmt.Printf("%s command needs source and target parameters\n", arg)
			return nil, nil
		case "cp":
			if argsLength-i == 1 {
				if !downloadFlag {
					fmt.Println("only source parameter can be used when download option is defined")
					return nil, nil
				}
				return &arg, []string{os.Args[argsLength]}
			}
			if argsLength-i == 2 {
				return &arg, []string{os.Args[argsLength-1], os.Args[argsLength]}
			}
			fmt.Printf("%s command needs source and target parameters or only source parameter when it used with download option\n", arg)
			return nil, nil
		case "mkdir":
			if argsLength-i == 1 {
				return &arg, []string{os.Args[argsLength]}
			}
			fmt.Printf("%s command needs target parameter\n", arg)
			return nil, nil
		case "rm":
			if argsLength-i == 0 {
				fmt.Printf("%s command needs target parameter\n", arg)
				return nil, nil
			}
			return &arg, os.Args[i+1:]
		case "ls":
			if argsLength-i < 2 {
				params := make([]string, 0)
				if argsLength-i == 0 {
					params = append(params, "/")
				} else {
					params = append(params, os.Args[argsLength])
				}
				return &arg, params
			}
			fmt.Printf("%s command needs source parameter\n", arg)
			return nil, nil
		}
	}
	return nil, nil
}

func defineFlags() *flagContainer {
	set := flag.NewFlagSet("dfs", flag.ContinueOnError)
	set.Usage = func() {
		printSupportedActions(set)
	}

	var headAddress string
	set.StringVar(&headAddress, `head-address`, "localhost:4000", `Points the end point of head node to work with.`)

	readRange := NewReadRange()
	set.Var(&readRange, `range`, `Reads the part of the file. Ex: 0-512`)

	/*set.Bool(`download`, false, `Writes the file to the current path with the original filename. Can be used only with cp command.
	Ex: -download cp [SOURCE]`)*/
	set.Bool(`overwrite`, false, `Ignores the existing file and continue the operation.`)

	if err := set.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	downloadFlag := strings.Index(strings.Join(os.Args, " "), "download") > -1

	command, params := grabAction(downloadFlag)
	if command == nil {
		set.Usage()
		os.Exit(1)
	}

	fc := &flagContainer{
		headAddress: headAddress,
		command:     *command,
		params:      params,
		//downloadFlag:  downloadFlag,
		overwriteFlag: strings.Index(strings.Join(os.Args, " "), "overwrite") > -1,
		//rangeFlag:     readRange,
	}

	if !fc.Validate() {
		set.Usage()
		os.Exit(2)
	}

	return fc
}
