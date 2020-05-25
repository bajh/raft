package store

import (
	"context"
	"fmt"
	"sync"
)

type Store interface {
	Get(context.Context, []byte) ([]byte, error)
	Set(context.Context, []byte, []byte) error
}

type MemStore struct {
	data map[string]string
	lock *sync.Mutex
}

type NotFoundError struct {
	Key []byte
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("key %s was not found", string(e.Key))
}

func (s MemStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	val, ok := s.data[string(key)]
	if !ok {
		return nil, NotFoundError{Key: key}
	}
	return []byte(val), nil
}

func (s MemStore) Set(ctx context.Context, key []byte, val []byte) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.data[string(key)] = string(val)
	return nil
}

func NewMemStore() Store {
	var lock sync.Mutex
	return MemStore{
		data: make(map[string]string),
		lock: &lock,
	}
}
