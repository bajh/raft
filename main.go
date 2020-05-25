package main

import (
	"context"
	"flag"
	"log"
	"raft/store"
	"raft/transport"
)

type Handler struct {
	store store.Store
}

func (s *Handler) HandleGet(ctx context.Context, c transport.Conn, key []byte) {
	val, err := s.store.Get(ctx, key)
	if err != nil {
		log.Println("error applying get:", err)
		// TODO: send error to client
		return
	}
	if err := c.WriteOpResultType(transport.OpResultTypeSuccess); err != nil {
		log.Println("error writing response type:", err)
		return
	}
	if err := c.Write(val); err != nil {
		log.Println("error writing val:", err)
		return
	}
	if err := c.Flush(); err != nil {
		log.Println("could not finish writing:", err)
	}
}

func (s *Handler) HandleSet(ctx context.Context, c transport.Conn, key []byte, val []byte) {
	if err := s.store.Set(ctx, key, val); err != nil {
		log.Println("error applying set:", err)
	}
	if err := c.WriteOpResultType(transport.OpResultTypeSuccess); err != nil {
		log.Println("error writing response type:", err)
		return
	}
	// TODO: send error to client
	if err := c.Flush(); err != nil {
		log.Println("could not finish writing:", err)
	}
}

func main() {
	port := flag.String("port", ":8999", "port for echo server")

	handler := &Handler{
		store: store.NewMemStore(),
	}
	server := transport.NewServer(handler)
	server.Listen(*port)
}
