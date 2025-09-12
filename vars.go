package rds

import (
	"sync/atomic"
)

type (
	RedisAtomicPointer[T any] struct {
		pointer atomic.Pointer[T]
		newT    func(client *RedisClient, batchSize ...int) *T
	}
)

func NewAtomicPointer[T any](newT func(client *RedisClient, batchSize ...int) *T) *RedisAtomicPointer[T] {
	return &RedisAtomicPointer[T]{newT: newT}
}

func (a *RedisAtomicPointer[T]) Get(client *RedisClient, batchSize ...int) *T {
	if t := a.pointer.Load(); t != nil {
		return t
	} else {
		t = a.newT(client, batchSize...)
		a.pointer.Store(t)
		return t
	}
}
