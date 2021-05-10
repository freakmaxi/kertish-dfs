package terminal

// Output interface is to handle multiple input operations for different output points
type Output interface {
	LockOrigin()
	PrintlnFromOrigin(input string)
	Println(input string)
	Printf(format string, args ...interface{})
	Print(input string)
	Remove(size int)
	Refresh()

	ActiveLine() string
	ActiveCommand() string
	MoveCursorHead()
	MoveCursorLeft(size int)
	MoveCursorRight(size int)
	MoveCursorLeftWord()
	MoveCursorRightWord()
	MoveCursorEnd()

	ScrollTop()
	ScrollPageUp()
	ScrollUp()
	ScrollDown()
	ScrollPageDown()
	ScrollBottom()

	Scan(out *string) bool
}
