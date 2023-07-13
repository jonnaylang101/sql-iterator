package iterator

import (
	"context"
	"database/sql"
	"fmt"
)

type Iterator[T, R any] interface {
	Iterate(ctx context.Context, query string, binder CustomBinder[T], worker WorkerFunc[T, R], options ...Option) ([]R, error)
}

type rowIterator[T, R any] struct {
	db     *sql.DB
	table  string
	result []R
}

func New[T, R any](db *sql.DB, table string) Iterator[T, R] {
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

	rows, err := ri.db.Query(query)
	if err != nil {
		return ri.result, fmt.Errorf("MakeSentencesFromDatabaseRows: error occurred while querying db: %v", err)
	}

	dbStream := genDataChansFromRows[T](ctx, rows, itOps.MaxBufferSize, binder)
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
