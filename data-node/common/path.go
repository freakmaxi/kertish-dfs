package common

import (
	"os"
)

func Traverse(p string, fileHandler func(info os.FileInfo) error) error {
	infos, err := os.ReadDir(p)
	if err != nil {
		return err
	}

	for _, info := range infos {
		if info.IsDir() || len(info.Name()) != 64 {
			continue
		}

		fi, err := info.Info()
		if err != nil {
			return err
		}

		if err := fileHandler(fi); err != nil {
			return err
		}
	}
	return nil
}
