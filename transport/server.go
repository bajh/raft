package transport

import (
	"context"
	"fmt"
	"log"
	"net"
)

type Handler interface {
	HandleGet(ctx context.Context, conn Conn, key []byte)
	HandleSet(ctx context.Context, conn Conn, key []byte, val []byte)
}

type Server struct {
	handler Handler
}

func (s *Server) Listen(port string) {
	ln, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("error accepting conn: %v\n", err)
			continue
		}
		go func(conn net.Conn) {
			c := NewConn(conn)
			ctx, cancel := context.WithCancel(context.Background())

			defer conn.Close()
			defer cancel()
			for {
				op, err := c.readOp(ctx)
				if err != nil {
					log.Println("error reading op:", err)
					return
				}

				switch op := op.(type) {
				case GetOp:
					s.handler.HandleGet(ctx, c, op.Key)
				case SetOp:
					s.handler.HandleSet(ctx, c, op.Key, op.Val)
				}

				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		}(conn)
	}
}

func NewServer(h Handler) Server {
	return Server{
		handler: h,
	}
}

func (c *Conn) readOpType() (OpType, error) {
	opType, err := c.mux.ReadByte()
	if err != nil {
		return 0, fmt.Errorf("reading op type: %v", err)
	}
	return OpType(opType), nil
}

func (c *Conn) readOp(ctx context.Context) (Op, error) {
	opType, err := c.readOpType()
	if err != nil {
		return nil, fmt.Errorf("reading op type: %v", err)
	}
	switch OpType(opType) {
	case opTypeGet:
		key, err := c.Read()
		if err != nil {
			return nil, fmt.Errorf("reading key: %v", err)
		}
		return GetOp{Key: key}, nil
	case opTypeSet:
		key, err := c.Read()
		if err != nil {
			return nil, fmt.Errorf("reading key: %v", err)
		}
		val, err := c.Read()
		if err != nil {
			return nil, fmt.Errorf("reading val: %v", err)
		}
		return SetOp{Key: key, Val: val}, nil
	}

	return nil, fmt.Errorf("invalid op type: %v", err)
}

func (c *Conn) WriteOpResultType(opResultType OpResultType) error {
	if err := c.mux.WriteByte(byte(opResultType)); err != nil {
		return fmt.Errorf("sending operation result type: %v", err)
	}
	return nil
}
