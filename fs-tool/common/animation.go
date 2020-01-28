package common

import (
	"sync"
	"time"

	"github.com/freakmaxi/kertish-dfs/fs-tool/terminal"
)

type animation struct {
	waitGroup sync.WaitGroup

	stop   chan bool
	cancel chan bool

	output terminal.Output
	header string
}

func NewAnimation(output terminal.Output, header string) *animation {
	return &animation{
		waitGroup: sync.WaitGroup{},
		stop:      make(chan bool),
		cancel:    make(chan bool),
		output:    output,
		header:    header,
	}
}

func (p *animation) Start() {
	p.output.Printf("%s |", p.header)

	p.waitGroup.Add(1)
	go func() {
		defer p.waitGroup.Done()

		chars := []byte{'/', '-', '\\', '|'}

		i := 0
		for {
			select {
			case <-p.stop:
				p.output.Remove(1)
				p.output.Printf("ok.\n")
				return
			case <-p.cancel:
				p.output.Remove(1)
				p.output.Printf("failed.\n")
				return
			default:
				c := chars[i%len(chars)]
				p.output.Remove(1)
				p.output.Printf(string(c))
				time.Sleep(time.Millisecond * 100)
			}
			i++
		}
	}()
}

func (p *animation) Stop() {
	p.stop <- true
	p.waitGroup.Wait()
}

func (p *animation) Cancel() {
	p.cancel <- true
	p.waitGroup.Wait()
}
