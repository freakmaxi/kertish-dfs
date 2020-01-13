package main

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/freakmaxi/2020-dfs/fs-tool/common"
	"github.com/freakmaxi/2020-dfs/fs-tool/dfs"
)

const local = "local:"

func main() {
	fc := defineFlags()

	switch fc.command {
	case "ls":
		if strings.Index(fc.params[0], local) == 0 {
			fmt.Println("please use O/S native commands to list files/folders")
			return
		}

		anim := common.NewAnimation("processing...")
		anim.Start()

		folder, err := dfs.List([]string{fc.headAddress}, fc.params[0])
		if err != nil {
			anim.Cancel()
			fmt.Println(err.Error())
			return
		}
		anim.Stop()

		fmt.Printf("total %d\n", len(folder.Folders)+len(folder.Files))
		for _, f := range folder.Folders {
			fmt.Printf("d %6vb %s %s\n", 0, f.Created.Format("2006 Jan 02 03:04"), f.Name)
		}
		for _, f := range folder.Files {
			lockChar := "-"
			if f.Locked {
				lockChar = "•"
			}
			fmt.Printf("%s %7v %s %s\n", lockChar, sizeToString(f.Size), f.Modified.Format("2006 Jan 02 03:04"), f.Name)
		}
	case "mkdir":
		if strings.Index(fc.params[0], local) == 0 {
			fmt.Println("please use O/S native commands to create folder(s)")
			return
		}

		anim := common.NewAnimation("processing...")
		anim.Start()

		if err := dfs.MakeFolder([]string{fc.headAddress}, fc.params[0]); err != nil {
			anim.Cancel()
			fmt.Println(err.Error())
			return
		}
		anim.Stop()
	case "cp", "mv":
		source := fc.params[0]

		if strings.Index(source, local) == 0 {
			/*if fc.downloadFlag {
				fmt.Println("please specify dfs path for source parameter")
				return
			}*/

			target := fc.params[1]
			if err := localToRemote(fc, source, target, strings.Compare(fc.command, "cp") == 0); err != nil {
				fmt.Println(err.Error())
				return
			}
			return
		}

		/*if fc.downloadFlag {
			downloadPath, _ := path.Split(os.Args[0])
			downloadPath = fmt.Sprintf("local:%s", downloadPath)

			if err := remoteToLocal(fc, source, downloadPath, true); err != nil {
				fmt.Println(err.Error())
				return
			}
			return
		}*/

		target := fc.params[1]
		if strings.Index(target, local) == 0 {
			if err := remoteToLocal(fc, source, target, strings.Compare(fc.command, "cp") == 0); err != nil {
				fmt.Println(err.Error())
				return
			}
			return
		}

		anim := common.NewAnimation("processing...")
		anim.Start()

		if err := dfs.Change([]string{fc.headAddress}, source, target, strings.Compare(fc.command, "cp") == 0); err != nil {
			anim.Cancel()
			fmt.Println(err.Error())
			return
		}
		anim.Stop()
	case "rm":
		list := ""
		for _, d := range fc.params {
			if strings.Index(d, local) == 0 {
				fmt.Println("please use O/S native commands to delete files/folders")
				return
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
			anim := common.NewAnimation("processing...")
			anim.Start()

			for _, d := range fc.params {
				if err := dfs.Delete([]string{fc.headAddress}, d); err != nil {
					anim.Cancel()
					fmt.Println(err.Error())
					return
				}
			}
			anim.Stop()
		}
	}
}

func sizeToString(size uint64) string {
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

func remoteToLocal(fc *flagContainer, source string, target string, copy bool) error {
	target = target[len(local):]

	info, err := os.Stat(target)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("local file can't open")
	}

	if info != nil && info.IsDir() {
		_, sourceFileName := path.Split(source)
		target = path.Join(target, sourceFileName)

		info, err = os.Stat(target)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("local file can't open")
		}
	}

	if info != nil {
		if info.IsDir() {
			return fmt.Errorf("target %s is a path", target)
		}

		if !fc.overwriteFlag {
			fmt.Printf("File %s is already exists\n", target)
			fmt.Print("Do you want to overwrite? (y/N) ")
			reader := bufio.NewReader(os.Stdin)
			char, _, err := reader.ReadRune()
			if err != nil {
				return err
			}

			switch char {
			case 'Y', 'y':
			default:
				return nil
			}
		}
	}

	anim := common.NewAnimation("processing...")
	anim.Start()

	if err := dfs.Pull([]string{fc.headAddress}, source, target); err != nil {
		anim.Cancel()
		return err
	}

	if !copy {
		if err := dfs.Delete([]string{fc.headAddress}, source); err != nil {
			anim.Cancel()
			return err
		}
	}

	anim.Stop()
	return nil
}

func localToRemote(fc *flagContainer, source string, target string, copy bool) error {
	if strings.Index(target, local) == 0 {
		return fmt.Errorf("please use O/S native commands to copy/move files/folders between local locations")
	}

	source = source[len(local):]
	if len(source) == 0 {
		return fmt.Errorf("please specify the source")
	}

	anim := common.NewAnimation("processing...")
	anim.Start()

	if err := dfs.Put([]string{fc.headAddress}, source, target, fc.overwriteFlag); err != nil {
		anim.Cancel()
		return fmt.Errorf(err.Error())
	}

	if !copy {
		if err := os.RemoveAll(source); err != nil {
			anim.Cancel()
			return fmt.Errorf("local file/folder couldn't delete")
		}
	}

	anim.Stop()
	return nil
}
