package iterator

import (
	"context"
)

type Iterator[T, R any] interface {
	Iterate(ctx context.Context, query string, binder CustomBinder[T], worker WorkerFunc[T, R], options ...Option) ([]R, error)
}

type rowIterator[T, R any] struct {
	db     Database[T]
	table  string
	result []R
}

type Database[T any] interface {
	Query(context.Context, string) (<-chan T, error)
}

func New[T, R any](db Database[T], table string) Iterator[T, R] {
	return &rowIterator[T, R]{
		db:    db,
		table: table,
	}
}

func (ri *rowIterator[T, R]) Iterate(ctx context.Context, query string, binder CustomBinder[T], worker WorkerFunc[T, R], options ...Option) ([]R, error) {
	itOps := IteratorOptions{
		MaxBufferSize: 12, // we can control how many rows to process at a time via the buffer size
		MaxProcesses:  12,
	}

	for _, opt := range options {
		opt(&itOps)
	}

	dbStream, err := ri.db.Query(ctx, query)
	if err != nil {
		return nil, err
	}

	chanStream := fanOut[T, R](ctx, dbStream, itOps.MaxProcesses, worker)
	stream := fanIn(ctx, chanStream)
	for item := range stream {
		ri.result = append(ri.result, item)
	}

	return ri.result, nil
}

type IteratorOptions struct {
	MaxBufferSize, MaxProcesses int
}

type Option func(*IteratorOptions)
