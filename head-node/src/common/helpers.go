package common

import (
	"fmt"
	"path"
	"strings"
)

const pathSeparator = "/"

func CorrectPaths(paths []string) []string {
	for i := range paths {
		folderPath := paths[i]

		if strings.Index(folderPath, pathSeparator) != 0 {
			folderPath = fmt.Sprintf("%s%s", pathSeparator, folderPath)
		}
		folderPath = path.Clean(folderPath)

		if len(folderPath) == 0 {
			folderPath = pathSeparator
		}

		paths[i] = folderPath
	}
	return paths
}

func CorrectPath(folderPath string) string {
	return CorrectPaths([]string{folderPath})[0]
}

func PathTree(folderPath string) []string {
	folderPath = CorrectPath(folderPath)

	folderTree := make([]string, 0)
	split := strings.Split(folderPath, pathSeparator)
	for len(split) > 0 {
		p := strings.Join(split, pathSeparator)
		folderTree = append([]string{CorrectPath(p)}, folderTree...)

		split = split[:len(split)-1]
	}

	return folderTree
}

func Split(path string) (string, string) {
	path = CorrectPath(path)
	if strings.Compare(path, pathSeparator) == 0 {
		return pathSeparator, ""
	}

	idx := strings.LastIndex(path, pathSeparator)
	if idx == 0 {
		return pathSeparator, path[1:]
	}

	return path[:idx], path[idx+1:]
}

func Join(inputs ...string) string {
	for i, p := range inputs {
		if strings.Index(p, pathSeparator) == 0 {
			p = p[1:]
		}
		inputs[i] = p
	}
	return CorrectPath(strings.Join(inputs, pathSeparator))
}

func ValidatePath(folderPath string) bool {
	return strings.Index(folderPath, "/") == 0
}
