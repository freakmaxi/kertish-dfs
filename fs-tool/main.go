package main

import (
	"fmt"
	"os"

	"github.com/freakmaxi/kertish-dfs/fs-tool/flags"
)

var version = "XX.X.XXXX"
var build = "XXXXXX"

func main() {
	command := flags.NewCommand(version, build, os.Args)
	if !command.Parse() {
		return
	}
	if err := command.Execute(); err != nil {
		fmt.Println(err.Error())
	}
}
