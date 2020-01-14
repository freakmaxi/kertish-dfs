package main

import (
	"fmt"
	"os"

	"github.com/freakmaxi/2020-dfs/fs-tool/flags"
)

func main() {
	command := flags.NewCommand(os.Args)
	if !command.Parse() {
		return
	}
	if err := command.Execute(); err != nil {
		fmt.Println(err.Error())
	}
}
