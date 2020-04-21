package common

import (
	"os"
	"strings"

	"github.com/freakmaxi/kertish-dfs/manager-node/src/errors"
)

type Tree struct {
	Folder *Folder
	Subs   []*Tree
}

func NewTree() *Tree {
	return newTree(nil)
}

func newTree(folder *Folder) *Tree {
	return &Tree{
		Folder: folder,
		Subs:   make([]*Tree, 0),
	}
}

func (t *Tree) Normalize() []*Folder {
	folders := make([]*Folder, 0)
	t.normalize(t, &folders)

	return folders
}

func (t *Tree) normalize(tree *Tree, folders *[]*Folder) {
	*folders = append(*folders, tree.Folder)

	if len(tree.Subs) == 0 {
		return
	}

	for _, subTree := range tree.Subs {
		t.normalize(subTree, folders)
	}
}

func (t *Tree) Fill(folders []*Folder) error {
	if len(folders) == 0 {
		return nil
	}

	t.Folder = folders[0]
	startIndex := 1
	if err := t.children(t, &startIndex, folders); err != nil {
		return err
	}
	return nil
}

func (t *Tree) children(parent *Tree, startIndex *int, folders []*Folder) error {
	for ; *startIndex < len(folders); *startIndex++ {
		folderFull := folders[*startIndex].Full
		parentName, name := Split(folders[*startIndex].Full)

		if strings.Compare(parent.Folder.Full, parentName) != 0 {
			if len(parent.Folder.Full) > len(parentName) {
				t.fixStructure(parent)
				return nil
			}

			// Something broken in the link Fix it
			var err error
			parent, err = t.fixTree(folders[*startIndex])
			if err != nil {
				return err
			}
			continue
		}

		if strings.Compare(parent.Folder.Full, parentName) == 0 {
			if !t.hasShadow(parent.Folder, folderFull) {
				if err := parent.Folder.NewFolder(name, func(shadow *FolderShadow) error {
					newFolder := NewFolder(shadow.Full)
					parent.Subs =
						append(parent.Subs, newTree(newFolder))
					return nil
				}); err != nil {
					return err
				}
			} else {
				parent.Subs = append(parent.Subs, newTree(folders[*startIndex]))
			}
			continue
		}

		refTree := t.get(parent, parentName)
		if refTree == nil {
			return errors.ErrNotFound
		}

		*startIndex += 1
		if err := t.children(refTree, startIndex, folders); err != nil {
			return err
		}
	}

	t.fixStructure(parent)

	return nil
}

func (t *Tree) hasShadow(searchingFolder *Folder, full string) bool {
	for _, folderShadow := range searchingFolder.Folders {
		if strings.Compare(folderShadow.Full, full) == 0 {
			return true
		}
	}
	return false
}

func (t *Tree) get(searchingTree *Tree, full string) *Tree {
	for _, tree := range searchingTree.Subs {
		if strings.Compare(tree.Folder.Full, full) == 0 {
			return tree
		}
	}
	return nil
}

func (t *Tree) fixTree(folder *Folder) (*Tree, error) {
	pathTree := PathTree(folder.Full)

	parentTree := t
	tree := t

	for i := 1; i < len(pathTree); i++ {
		p := pathTree[i]

		treeBackup := t.get(tree, p)
		if treeBackup == nil {
			_, name := Split(p)

			if err := tree.Folder.NewFolder(name, func(shadow *FolderShadow) error {
				return nil
			}); err != nil && err != os.ErrExist {
				return nil, err
			}

			if strings.Compare(folder.Full, p) == 0 {
				treeBackup = newTree(folder)
			} else {
				newFolder := NewFolder(p)
				treeBackup = newTree(newFolder)
			}

			tree.Subs = append(tree.Subs, treeBackup)
		}
		parentTree = tree
		tree = treeBackup
	}

	return parentTree, nil
}

func (t *Tree) fixStructure(tree *Tree) {
	for _, folderShadow := range tree.Folder.Folders {
		exists := false
		for _, treeItem := range tree.Subs {
			if strings.Compare(treeItem.Folder.Full, folderShadow.Full) == 0 {
				exists = true
				break
			}
		}
		if !exists {
			folder := NewFolder(folderShadow.Full)
			tree.Subs = append(tree.Subs, newTree(folder))
		}
	}
}
