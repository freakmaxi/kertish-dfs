package common

import (
	"fmt"
	"sync"
	"time"
)

type animation struct {
	waitGroup sync.WaitGroup

	stop   chan bool
	cancel chan bool

	header string
}

func NewAnimation(header string) *animation {
	return &animation{
		waitGroup: sync.WaitGroup{},
		stop:      make(chan bool),
		cancel:    make(chan bool),
		header:    header,
	}
}

func (p *animation) Start() {
	fmt.Printf("%s |", p.header)

	p.waitGroup.Add(1)
	go func() {
		defer p.waitGroup.Done()

		chars := []byte{'/', '-', '\\', '|'}

		i := 0
		for {
			select {
			case <-p.stop:
				fmt.Printf("\033[%dD", 1)
				fmt.Printf("ok.\n")
				return
			case <-p.cancel:
				fmt.Printf("\033[%dD", 1)
				fmt.Printf("failed.\n")
				return
			default:
				c := chars[i%len(chars)]
				fmt.Printf("\033[%dD", 1)
				fmt.Printf(string(c))
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
