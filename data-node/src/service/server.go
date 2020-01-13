package service

import (
	"fmt"
	"net"
)

type Server interface {
	Listen() error
	Kill() error
}

type server struct {
	address   *net.TCPAddr
	commander Commander

	listener *net.TCPListener
	quiting  bool
}

func NewServer(address string, c Commander) (Server, error) {
	if len(address) == 0 {
		return nil, fmt.Errorf("address should be defined")
	}
	addr, _ := net.ResolveTCPAddr("tcp4", address)

	return &server{
		address:   addr,
		commander: c,
	}, nil
}

func (s *server) Listen() error {
	var err error
	s.listener, err = net.ListenTCP("tcp4", s.address)
	if err != nil {
		return err
	}

	for !s.quiting {
		c, err := s.listener.Accept()
		if err != nil {
			fmt.Printf("ERROR: Unable to accept connection: %s\n", err.Error())
			continue
		}
		go s.commander.Handler(c)
	}

	return nil
}

func (s *server) Kill() error {
	s.quiting = true

	if s.listener == nil {
		return nil
	}

	if err := s.listener.Close(); err != nil {
		return err
	}

	return nil
}

var _ Server = &server{}
