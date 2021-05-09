package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

/**
/
/Level11
/Level12               <<<< MISSING IN THE PARENT'S DEFINITION
/Level11/Level2
/Level12/Level2		   <<<< MISSING IN METADATA
/Level11/Level2/Level3 <<<< MISSING IN THE PARENT'S DEFINITION
/Level11/Level2/Level4
*/
func TestTree_FillV1(t *testing.T) {
	folders := make([]*Folder, 0)
	root := NewFolder("/")
	level11, _ := root.NewFolder("Level11")
	folders = append(folders, root)

	_, _ = level11.NewFolder("Level2")
	folders = append(folders, level11)

	level12 := NewFolder("/Level12")
	_, _ = level12.NewFolder("Level2")
	folders = append(folders, level12)

	level2 := NewFolder("/Level11/Level2")
	_, _ = level2.NewFolder("Level4")
	folders = append(folders, level2)

	folders = append(folders, NewFolder("/Level11/Level2/Level3"))
	folders = append(folders, NewFolder("/Level11/Level2/Level4"))

	tree := NewTree()
	_ = tree.Fill(nil, folders)

	normalized := tree.Normalize()

	result := ""
	for _, folder := range normalized {
		result = fmt.Sprintf("%s$P:%s", result, folder.Full)
		for _, folderShadow := range folder.Folders {
			result = fmt.Sprintf("%s$C:%s", result, folderShadow.Full)
		}
	}

	expected := "$P:/$C:/Level11$C:/Level12$P:/Level11$C:/Level11/Level2$P:/Level11/Level2$C:/Level11/Level2/Level3$C:/Level11/Level2/Level4$P:/Level11/Level2/Level3$P:/Level11/Level2/Level4$P:/Level12$C:/Level12/Level2$P:/Level12/Level2"
	assert.Equal(t, expected, result)
}

/**
/
/Level11
/Level12
/Level11/Level2
/Level12/Level2
/Level11/Level2/Level3		<<<< MISSING IN THE PARENT'S DEFINITION
/Level11/Level2/Level4
/Level12/Level2/Level4  	<<<< MISSING IN METADATA
/Orphan/Path/For/Test   	<<<< ORPHAN
/Orphan/Path/For/Test/REG	<<<< MISSING IN METADATA
*/
func TestTree_FillV2(t *testing.T) {
	folders := make([]*Folder, 0)
	root := NewFolder("/")
	level11, _ := root.NewFolder("Level11")
	folders = append(folders, root)

	_, _ = level11.NewFolder("Level2")
	folders = append(folders, level11)

	level12 := NewFolder("/Level12")
	_, _ = level12.NewFolder("Level2")
	folders = append(folders, level12)

	level21 := NewFolder("/Level11/Level2")
	_, _ = level21.NewFolder("Level4")
	folders = append(folders, level21)

	level22 := NewFolder("/Level12/Level2")
	_, _ = level22.NewFolder("Level4")
	folders = append(folders, level22)

	folders = append(folders, NewFolder("/Level11/Level2/Level3"))
	folders = append(folders, NewFolder("/Level11/Level2/Level4"))

	orphan := NewFolder("/Orphan/Path/For/Test")
	_, _ = orphan.NewFolder("REG")
	folders = append(folders, orphan)

	tree := NewTree()
	_ = tree.Fill(nil, folders)

	normalized := tree.Normalize()

	result := ""
	for _, folder := range normalized {
		result = fmt.Sprintf("%s$P:%s", result, folder.Full)
		for _, folderShadow := range folder.Folders {
			result = fmt.Sprintf("%s$C:%s", result, folderShadow.Full)
		}
	}

	expected := "$P:/$C:/Level11$C:/Level12$C:/Orphan$P:/Level11$C:/Level11/Level2$P:/Level11/Level2$C:/Level11/Level2/Level3$C:/Level11/Level2/Level4$P:/Level11/Level2/Level3$P:/Level11/Level2/Level4$P:/Level12$C:/Level12/Level2$P:/Level12/Level2$C:/Level12/Level2/Level4$P:/Level12/Level2/Level4$P:/Orphan$C:/Orphan/Path$P:/Orphan/Path$C:/Orphan/Path/For$P:/Orphan/Path/For$C:/Orphan/Path/For/Test$P:/Orphan/Path/For/Test$C:/Orphan/Path/For/Test/REG$P:/Orphan/Path/For/Test/REG"
	assert.Equal(t, expected, result)
}
