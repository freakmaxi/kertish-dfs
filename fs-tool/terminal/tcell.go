package terminal

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell"
	"github.com/mattn/go-runewidth"
)

type tCellOut struct {
	screen    tcell.Screen
	column    int
	colOrigin int

	rows          []string
	firstRowIndex int

	scrolled bool
}

func NewTCellOut(screen tcell.Screen) Output {
	o := &tCellOut{
		screen:        screen,
		column:        0,
		colOrigin:     0,
		rows:          make([]string, 0),
		firstRowIndex: 0,
	}
	o.addNewRow()

	return o
}

func (t *tCellOut) addNewRow() {
	t.rows = append(t.rows, "")
	t.column = 0

	_, height := t.screen.Size()
	if len(t.rows) >= height {
		t.firstRowIndex = len(t.rows) - height
	}
}

func (t *tCellOut) activeRowIndex() int {
	rowsLength := len(t.rows)
	if t.firstRowIndex == 0 {
		return rowsLength - 1
	}

	_, height := t.screen.Size()
	return (t.firstRowIndex + height) - 1
}

func (t *tCellOut) activeRowScreenIndex() int {
	rowsLength := len(t.rows) - 1
	return rowsLength - t.firstRowIndex
}

func (t *tCellOut) rowLength(rowIndex int) int {
	row := t.rows[rowIndex]
	return len([]rune(row))
}

func (t *tCellOut) LockOrigin() {
	row := t.ActiveLine()
	runeRow := []rune(row)
	t.colOrigin = len(runeRow) + t.colOrigin
}

func (t *tCellOut) PrintlnFromOrigin(input string) {
	shiftS := fmt.Sprintf(fmt.Sprintf("%%%dv", t.colOrigin), " ")
	input = fmt.Sprintf("%s%s", shiftS, input)
	t.Println(input)
}

func (t *tCellOut) Println(input string) {
	if len(input) == 0 {
		t.addNewRow()
		return
	}

	if t.column > 0 {
		t.addNewRow()
	}
	t.Print(input)
	t.addNewRow()
}

func (t *tCellOut) Printf(format string, args ...interface{}) {
	input := fmt.Sprintf(format, args...)
	t.Print(input)
}

func (t *tCellOut) Print(input string) {
	appendFunc := func(row string, index int, runeInput []rune) (string, int) {
		runeRow := []rune(row)

		newRuneRow := make([]rune, 0)
		newRuneRow = append(newRuneRow, runeRow[:index]...)
		newRuneRow = append(newRuneRow, runeInput...)
		newRuneRow = append(newRuneRow, runeRow[index:]...)
		return string(newRuneRow), index + len(runeInput)
	}

	if t.scrolled {
		t.ScrollBottom()
	}

	width, _ := t.screen.Size()
	rowIndex := t.activeRowIndex()
	rowLength := t.rowLength(rowIndex)

	index := 0
	runeInput := []rune(input)
	for len(runeInput[index:]) > 0 {
		v := runeInput[index:][0]
		index++

		if v != 10 && rowLength+index < width {
			continue
		}

		t.rows[rowIndex], _ = appendFunc(t.rows[rowIndex], t.column, runeInput[:index-1])
		t.addNewRow()

		runeInput = runeInput[index:]
		index = 0
		rowLength = 0
		rowIndex++
	}
	t.rows[rowIndex], t.column = appendFunc(t.rows[rowIndex], t.column, runeInput)
}

func (t *tCellOut) Remove(size int) {
	removeFunc := func(row string, index int, size int) (string, int) {
		if index-size < 0 {
			size = index
		}
		if index-size < t.colOrigin {
			return row, index
		}
		index -= size

		runeRow := []rune(row)
		newRuneRow := make([]rune, 0)
		newRuneRow = append(newRuneRow, runeRow[:index]...)
		newRuneRow = append(newRuneRow, runeRow[index+size:]...)
		return string(newRuneRow), index
	}

	rowIndex := t.activeRowIndex()
	t.rows[rowIndex], t.column = removeFunc(t.rows[rowIndex], t.column, size)
}

func (t *tCellOut) Refresh() {
	t.draw()

	rowScreenIndex := t.activeRowScreenIndex()
	t.screen.ShowCursor(t.column, rowScreenIndex)
	t.screen.Sync()
}

func (t *tCellOut) ActiveLine() string {
	rowIndex := t.activeRowIndex()
	row := t.rows[rowIndex]
	runeRow := []rune(row)
	return string(runeRow[t.colOrigin:])
}

func (t *tCellOut) MoveCursorHead() {
	t.column = t.colOrigin
	rowScreenIndex := t.activeRowScreenIndex()
	t.screen.ShowCursor(t.column, rowScreenIndex)
	t.screen.Sync()
}

func (t *tCellOut) MoveCursorLeft(size int) {
	if t.column-size < 0 {
		size = t.column
	}
	t.column -= size
	if t.column < t.colOrigin {
		t.column = t.colOrigin
	}

	rowScreenIndex := t.activeRowScreenIndex()
	t.screen.ShowCursor(t.column, rowScreenIndex)
	t.screen.Sync()
}

func (t *tCellOut) MoveCursorRight(size int) {
	rowIndex := t.activeRowIndex()
	rowLength := t.rowLength(rowIndex)

	if t.column+size >= rowLength {
		size = rowLength - t.column
	}
	t.column += size

	rowScreenIndex := t.activeRowScreenIndex()
	t.screen.ShowCursor(t.column, rowScreenIndex)
	t.screen.Sync()
}

func (t *tCellOut) MoveCursorLeftWord() {
	row := t.ActiveLine()
	row = row[:t.column-t.colOrigin]

	bufferParts := strings.Split(row, " ")
	lastPartLength := 0
	for len(bufferParts) > 0 {
		bufferPartsLength := len(bufferParts)

		lastPart := bufferParts[bufferPartsLength-1]
		if len(lastPart) == 0 {
			bufferParts = bufferParts[:bufferPartsLength-1]
			lastPartLength++
			continue
		}

		lastPartLength +=
			runewidth.StringWidth(lastPart)
		break
	}
	t.MoveCursorLeft(lastPartLength)
}

func (t *tCellOut) MoveCursorRightWord() {
	row := t.ActiveLine()
	row = row[t.column-t.colOrigin:]

	bufferParts := strings.Split(row, " ")
	lastPartLength := 0
	for len(bufferParts) > 0 {
		lastPart := bufferParts[0]
		if len(lastPart) == 0 {
			bufferParts = bufferParts[1:]
			lastPartLength++
			continue
		}

		lastPartLength +=
			runewidth.StringWidth(lastPart)
		break
	}
	t.MoveCursorRight(lastPartLength)
}

func (t *tCellOut) MoveCursorEnd() {
	rowIndex := t.activeRowIndex()
	row := t.rows[rowIndex]
	t.column = len([]rune(row))
	if t.column == 0 {
		t.column = t.colOrigin
	}

	rowScreenIndex := t.activeRowScreenIndex()
	t.screen.ShowCursor(t.column, rowScreenIndex)
	t.screen.Sync()
}

func (t *tCellOut) ScrollTop() {
	t.firstRowIndex = 0
	t.draw()
	t.screen.Sync()
	t.scrolled = true
}

func (t *tCellOut) ScrollPageUp() {
	_, height := t.screen.Size()
	t.firstRowIndex -= height
	if t.firstRowIndex < 0 {
		t.firstRowIndex = 0
	}

	t.draw()
	t.screen.Sync()

	t.scrolled = true
}

func (t *tCellOut) ScrollUp() {
	if t.firstRowIndex == 0 {
		return
	}
	t.firstRowIndex--
	t.draw()
	t.screen.Sync()

	t.scrolled = true
}

func (t *tCellOut) ScrollDown() {
	_, height := t.screen.Size()

	if t.firstRowIndex >= len(t.rows)-height {
		return
	}
	t.firstRowIndex++
	t.draw()
	t.screen.Sync()

	t.scrolled = true
}

func (t *tCellOut) ScrollPageDown() {
	_, height := t.screen.Size()
	t.firstRowIndex += height
	if t.firstRowIndex >= len(t.rows)-height {
		t.firstRowIndex = len(t.rows) - height
	}

	t.draw()
	t.screen.Sync()

	t.scrolled = true
}

func (t *tCellOut) ScrollBottom() {
	_, height := t.screen.Size()

	t.scrolled = false
	lastRowNum := len(t.rows)

	t.firstRowIndex = lastRowNum - height
	if t.firstRowIndex < 0 {
		t.firstRowIndex = 0
	}
	t.draw()
	t.screen.Sync()
}

func (t *tCellOut) Scan(out *string) bool {
	currentColumnOrigin := t.colOrigin
	t.LockOrigin()
	defer func() {
		t.colOrigin = currentColumnOrigin
	}()

	*out = ""
	for {
		ev := t.screen.PollEvent()
		switch ev := ev.(type) {
		case *tcell.EventKey:
			switch ev.Key() {
			case tcell.KeyEscape:
				*out = ""
				return false
			case tcell.KeyEnter:
				t.addNewRow()
				return true
			case tcell.KeyBackspace2:
				w := *out
				if len(w) > 0 {
					w = w[:len(w)-1]
				}
				*out = w
				t.Remove(1)
				t.Refresh()
			default:
				r := ev.Rune()

				w := *out
				w = fmt.Sprintf("%s%s", w, string(r))
				*out = w

				t.Print(string(r))
				t.Refresh()
			}
		}
	}
}

func (t *tCellOut) draw() {
	width, _ := t.screen.Size()
	/*if len(t.rows) >= height {
		t.firstRowIndex = len(t.rows) - height
	}*/

	for rowIndex, line := range t.rows[t.firstRowIndex:] {
		columnIndex := 0
		runeLine := []rune(line)
		for len(runeLine) > 0 {
			runeLength := runewidth.RuneWidth(runeLine[0])
			if runeLength == 0 {
				runeLine = runeLine[1:]
				continue
			}
			var tail []rune = nil
			if runeLength > 1 {
				tail = runeLine[1 : runeLength-1]
			}
			t.screen.SetContent(columnIndex, rowIndex, runeLine[0], tail, tcell.StyleDefault)
			columnIndex++
			runeLine = runeLine[runeLength:]
		}

		if columnIndex < width {
			for emptyColumnIndex := columnIndex; emptyColumnIndex < width; emptyColumnIndex++ {
				t.screen.SetContent(emptyColumnIndex, rowIndex, ' ', nil, tcell.StyleDefault)
			}
		}
	}
}

var _ Output = &tCellOut{}
