package common

import (
	"io/ioutil"
	"os"
)

func Traverse(p string, fileHandler func(info os.FileInfo) error) error {
	infos, err := ioutil.ReadDir(p)
	if err != nil {
		return err
	}

	for _, info := range infos {
		if info.IsDir() || len(info.Name()) != 64 {
			continue
		}

		if err := fileHandler(info); err != nil {
			return err
		}
	}
	return nil
}
