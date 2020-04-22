package terminal

import "fmt"

type stdout struct {
	column      int
	colBlockIdx int
}

func NewStdOut() Output {
	return &stdout{
		column:      0,
		colBlockIdx: 0,
	}
}

func (s *stdout) LockOrigin() {
	s.colBlockIdx = s.column
}

func (s *stdout) PrintlnFromOrigin(input string) {
	shiftS := fmt.Sprintf(fmt.Sprintf("%%%dv", s.colBlockIdx), " ")
	input = fmt.Sprintf("%s%s", shiftS, input)
	s.Println(input)
}

func (s *stdout) Println(input string) {
	fmt.Println(input)
	s.column = s.colBlockIdx
}

func (s *stdout) Printf(format string, args ...interface{}) {
	s.Print(fmt.Sprintf(format, args...))
}

func (s *stdout) Print(input string) {
	fmt.Print(input)
	if input[len(input)-1] != 10 {
		s.column += len(input)
		return
	}
	s.column = s.colBlockIdx
}

func (s *stdout) Remove(size int) {
	if s.column-size < 0 {
		size = s.column
	}
	s.column -= size
	if s.column < s.colBlockIdx {
		size -= s.colBlockIdx - s.column
	}
	fmt.Printf("\033[%dD", size)
	s.column -= size
}

func (s *stdout) Refresh() {
}

func (s *stdout) ActiveLine() string {
	return ""
}

func (s *stdout) MoveCursorHead() {
}

func (s *stdout) MoveCursorLeft(size int) {
}

func (s *stdout) MoveCursorRight(size int) {
}

func (s *stdout) MoveCursorLeftWord() {
}

func (s *stdout) MoveCursorRightWord() {
}

func (s *stdout) MoveCursorEnd() {
}

func (s *stdout) ScrollTop() {
}

func (s *stdout) ScrollPageUp() {
}

func (s *stdout) ScrollUp() {
}

func (s *stdout) ScrollDown() {
}

func (s *stdout) ScrollPageDown() {
}

func (s *stdout) ScrollBottom() {
}

func (s *stdout) Scan(out *string) bool {
	if _, err := fmt.Scan(out); err != nil {
		return false
	}
	return true
}

var _ Output = &stdout{}
