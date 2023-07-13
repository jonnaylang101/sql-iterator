package iterator

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Iterator[T, R any] interface {
	Iterate(ctx context.Context, query string, binder customBinder[T], worker workerFunc[T, R], options ...Option) ([]R, error)
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

func (ri *rowIterator[T, R]) Iterate(ctx context.Context, query string, binder customBinder[T], worker workerFunc[T, R], options ...Option) ([]R, error) {
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

type DbResult struct {
	Name string
	Age  int
	Err  error
}

type SentenceResult struct {
	Sentence string
	Err      error
}

type IteratorOptions struct {
	MaxBufferSize, MaxProcesses int
}

type Option func(*IteratorOptions)

var DbResultBinder customBinder[DbResult] = func(rows *sql.Rows) DbResult {
	d := DbResult{}
	d.Err = rows.Scan(&d.Name, &d.Age)

	return d
}

var SentenceWorker workerFunc[DbResult, SentenceResult] = func(ctx context.Context, in DbResult) SentenceResult {
	time.Sleep(time.Millisecond * 500) // emulate a longer running process
	return SentenceResult{
		Err:      in.Err,
		Sentence: fmt.Sprintf("This is %s, they are %d years old", in.Name, in.Age),
	}
}
