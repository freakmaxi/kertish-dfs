package common

import (
	"os"
	"sort"
	"strings"
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
	currentTree := t

	for i := 1; i < len(folders); i++ {
		folder := folders[i]

		folderFull := folder.Full
		parentPath, name := Split(folderFull)

		if strings.Compare(currentTree.Folder.Full, parentPath) == 0 {
			if !t.hasShadow(currentTree.Folder, folderFull) {
				_ = currentTree.Folder.NewFolder(name, func(shadow *FolderShadow) error {
					return nil
				})
				sort.Sort(currentTree.Folder.Folders)
			}
			currentTree.Subs = append(currentTree.Subs, newTree(folder))
			continue
		}

		if _, err := t.locateTree(folder); err != nil { // ErrNotExists all the time
			// Something broken in the tree structure, fill missing parts
			if err := t.fixTree(currentTree, folder); err != nil {
				return err
			}
		}
		currentTree, _ = t.locateTree(folder)
	}
	t.ensureStructure(t)

	return nil
}

func (t *Tree) locateTree(folder *Folder) (*Tree, error) {
	parts := PathTree(folder.Full)

	currentTree := t
	for i := 0; i < len(parts); i++ {
		part := parts[i]

		if strings.Compare(currentTree.Folder.Full, part) == 0 {
			continue
		}

		currentTree = t.get(currentTree, parts[i])
		if currentTree == nil {
			return nil, os.ErrNotExist
		}
	}
	return currentTree, nil
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

func (t *Tree) fixTree(parent *Tree, folder *Folder) error {
	pathTree := PathTree(folder.Full)

	for len(pathTree) > 0 {
		p := pathTree[0]

		if strings.Compare(parent.Folder.Full, p) != 0 {
			pathTree = pathTree[1:]
			continue
		}
		pathTree = pathTree[1:]
		break
	}

	if len(pathTree) == 0 {
		return t.fixTree(t, folder)
	}

	tree := parent
	for i := 0; i < len(pathTree); i++ {
		p := pathTree[i]

		childTree := t.get(tree, p)
		if childTree != nil {
			tree = childTree
			continue
		}

		_, name := Split(p)

		if err := tree.Folder.NewFolder(name, func(shadow *FolderShadow) error {
			return nil
		}); err != nil && err != os.ErrExist {
			return err
		}
		sort.Sort(tree.Folder.Folders)

		if strings.Compare(folder.Full, p) == 0 {
			childTree = newTree(folder)
		} else {
			childTree = newTree(NewFolder(p))
		}
		tree.Subs = append(tree.Subs, childTree)
		tree = childTree
	}

	return nil
}

func (t *Tree) ensureStructure(tree *Tree) {
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

	for _, treeItem := range tree.Subs {
		t.ensureStructure(treeItem)
	}
}
