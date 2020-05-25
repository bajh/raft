package transport

import (
	"fmt"
	"net"
)

type Client struct {
	conn Conn
}

func (c *Client) WriteOpType(opType OpType) error {
	if err := c.conn.mux.WriteByte(byte(opType)); err != nil {
		return fmt.Errorf("sending operation type: %v", err)
	}
	return nil
}

func (c *Client) ReadOpResultType() (OpResultType, error) {
	opResultType, err := c.conn.mux.ReadByte()
	if err != nil {
		return 0, fmt.Errorf("reading op result type: %v", err)
	}
	return OpResultType(opResultType), nil
}

func NewClient(addr string) (*Client, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connecting to host: %v", err)
	}
	return &Client{
		conn: NewConn(c),
	}, nil
}

func (c *Client) Get(key []byte) ([]byte, error) {
	if err := c.WriteOpType(opTypeGet); err != nil {
		return nil, fmt.Errorf("sending operation type: %v", err)
	}

	if err := c.conn.Write(key); err != nil {
		return nil, fmt.Errorf("sending key size: %v", err)
	}

	if err := c.conn.mux.Flush(); err != nil {
		return nil, fmt.Errorf("flushing data: %v", err)
	}

	// now read the response
	opResult, err := c.ReadOpResultType()
	if err != nil {
		return nil, fmt.Errorf("reading response: %v", err)
	}
	if opResult != OpResultTypeSuccess {
		// TODO: read the error message
		return nil, fmt.Errorf("error")
	}

	val, err := c.conn.Read()
	if err != nil {
		return nil, fmt.Errorf("reading val: %v", err)
	}
	return val, nil
}

func (c *Client) Set(key []byte, val []byte) error {
	if err := c.WriteOpType(opTypeSet); err != nil {
		return fmt.Errorf("sending operation type: %v", err)
	}
	if err := c.conn.Write(key); err != nil {
		return fmt.Errorf("sending key: %v", err)
	}
	if err := c.conn.Write(val); err != nil {
		return fmt.Errorf("sending val: %v", err)
	}

	if err := c.conn.mux.Flush(); err != nil {
		return fmt.Errorf("flushing data: %v", err)
	}

	// now read the response
	opResult, err := c.ReadOpResultType()
	if err != nil {
		return fmt.Errorf("reading response: %v", err)
	}
	if opResult != OpResultTypeSuccess {
		// TODO: read the error message
		return fmt.Errorf("error")
	}

	return nil
}
