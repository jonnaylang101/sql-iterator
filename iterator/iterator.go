package iterator

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type dbResult struct {
	name string
	age  int
	err  error
}

type sentenceResult struct {
	sentence string
	err      error
}

// I've forgone my normal TDD ways to get this idea down as quickly as possible.

type IteratorOptions struct {
	MaxBufferSize, MaxProcesses int
}

type Option func(*IteratorOptions)

func MakeSentencesFromDatabaseRows(ctx context.Context, db *sql.DB, table string, options ...Option) ([]string, error) {
	itOps := IteratorOptions{
		MaxBufferSize: 12, // we can control how many rows to process at a time via the buffer size
		MaxProcesses:  12,
	}

	for _, opt := range options {
		opt(&itOps)
	}

	out := make([]string, 0)

	query := `SELECT firstname, age FROM ` + table
	rows, err := db.Query(query)
	if err != nil {
		return out, fmt.Errorf("MakeSentencesFromDatabaseRows: error occurred while querying db: %v", err)
	}

	var makeSentence workerFunc[sentenceResult] = func(ctx context.Context, in dbResult) sentenceResult {
		time.Sleep(time.Millisecond * 500) // emulate a longer running process
		return sentenceResult{
			err:      in.err,
			sentence: fmt.Sprintf("This is %s, they are %d years old", in.name, in.age),
		}
	}

	dbStream := genDataChansFromRows(ctx, rows, itOps.MaxBufferSize)
	chanStream := fanOut(ctx, dbStream, itOps.MaxProcesses, makeSentence)
	stream := fanIn(ctx, chanStream)
	for item := range stream {
		if item.err != nil {
			return out, fmt.Errorf("MakeSentencesFromDatabaseRows: error occurred while parsing rows: %v", err)
		}
		out = append(out, item.sentence)
	}

	return out, nil
}
