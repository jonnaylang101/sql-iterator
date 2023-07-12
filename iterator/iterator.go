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

func MakeSentencesFromDatabaseRows(db *sql.DB, table string) ([]string, error) {
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

	maxBufferSize := 12 // we can control how many rows to process at a time via the buffer size
	maxProcesses := 12
	ctx := context.Background()
	dbStream := genDataChansFromRows(rows, maxBufferSize)
	chanStream := fanOut(ctx, dbStream, maxProcesses, makeSentence)
	stream := fanIn(ctx, chanStream)
	for item := range stream {
		if item.err != nil {
			return out, fmt.Errorf("MakeSentencesFromDatabaseRows: error occurred while parsing rows: %v", err)
		}
		out = append(out, item.sentence)
	}

	return out, nil
}
