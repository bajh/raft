package transport

import (
	"bufio"
	"fmt"
	"io"
	"net"
)

type Op interface{}

type GetOp struct {
	Key []byte
}

type SetOp struct {
	Key []byte
	Val []byte
}

type OpType byte

const (
	opTypeGet OpType = iota + 1
	opTypeSet
	opTypeDelete
)

type OpResultType byte

const (
	OpResultTypeSuccess OpResultType = iota + 1
	OpResultTypeError
)

type Conn struct {
	mux mux
}

func NewConn(c net.Conn) Conn {
	return Conn{
		mux: bufio.NewReadWriter(bufio.NewReader(c), bufio.NewWriter(c)),
	}
}

func (c *Conn) Read() ([]byte, error) {
	size, err := c.mux.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("reading val size: %v", err)
	}
	// TODO: make this buffer reusable probably?
	val := make([]byte, size)
	n, err := c.mux.Read(val)
	if err != nil {
		return nil, fmt.Errorf("reading val: %v", err)
	}
	if n != int(size) {
		return nil, fmt.Errorf("expected val size %d, got %d", size, n)
	}
	return val, nil
}

func (c *Conn) Write(val []byte) error {
	if err := c.mux.WriteByte(byte(len(val))); err != nil {
		return fmt.Errorf("sending val size: %v", err)
	}

	n, err := c.mux.Write(val)
	if err != nil {
		return fmt.Errorf("sending val: %v", err)
	}
	if n != len(val) {
		return fmt.Errorf("could not send full val: %v", err)
	}
	return nil
}

func (c *Conn) Flush() error {
	return c.mux.Flush()
}

type mux interface {
	io.Reader
	io.Writer
	io.ByteReader
	io.ByteWriter
	Flush() error
}
