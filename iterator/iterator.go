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

	var makeSentence WorkerFunc[sentenceResult] = func(ctx context.Context, in dbResult) sentenceResult {
		time.Sleep(time.Millisecond * 3500) // emulate a longer running process
		return sentenceResult{
			err:      in.err,
			sentence: fmt.Sprintf("This is %s, they are %d years old", in.name, in.age),
		}
	}

	maxBufferSize := 12
	maxProcesses := 12
	ctx := context.Background()
	dbStream := GenDataChansFromRows(rows, maxBufferSize)
	chanStream := FanOut(ctx, dbStream, maxProcesses, makeSentence)
	stream := FanIn(ctx, chanStream)

	for item := range stream {
		if item.err != nil {
			return out, fmt.Errorf("MakeSentencesFromDatabaseRows: error occurred while parsing rows: %v", err)
		}
		fmt.Println(item.sentence)
	}

	return out, nil
}

func GenDataChansFromRows(rows *sql.Rows, bufferSize int) <-chan dbResult {
	outStream := make(chan dbResult, bufferSize)

	go func() {
		defer close(outStream)

		var name string
		var age int
		for rows.Next() {
			err := rows.Scan(&name, &age)
			outStream <- dbResult{
				name: name,
				age:  age,
				err:  err,
			}
		}
	}()

	return outStream
}

type WorkerFunc[Res any] func(ctx context.Context, in dbResult) Res

func FanOut[O any](ctx context.Context, inStream <-chan dbResult, maxProcs int, worker WorkerFunc[O]) <-chan <-chan O {
	chanStream := make(chan (<-chan O), maxProcs)
	if inStream == nil || worker == nil {
		close(chanStream)
		panic("FanOut: check args for nil values")
	}

	go func() {
		defer close(chanStream)
		for i := 0; i < maxProcs; i++ {
			select {
			case <-ctx.Done():
				return
			case chanStream <- WorkerThread(ctx, inStream, worker):
			}
		}

	}()

	return chanStream
}

func WorkerThread[Out any](ctx context.Context, inStream <-chan dbResult, workerFunc WorkerFunc[Out]) <-chan Out {
	resStream := make(chan Out)
	if inStream == nil {
		close(resStream)
		panic("WorkerThread: provided stream has nil value")
	}

	go func() {
		defer close(resStream)
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-inStream:
				if !ok {
					return
				}
				select {
				case resStream <- workerFunc(ctx, item):
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return resStream
}

func FanIn[T any](ctx context.Context, chanStream <-chan (<-chan T)) chan T {
	outStream := make(chan T)
	if chanStream == nil {
		close(outStream)
		panic("FanIn: chanStream has nil value")
	}

	go func() {
		defer close(outStream)
		for {
			var possStream <-chan T
			select {
			case chn, ok := <-chanStream:
				if !ok {
					return
				}
				possStream = chn
			case <-ctx.Done():
				return
			}
			for t := range OrDone(ctx, possStream) {
				select {
				case outStream <- t:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return outStream
}

func OrDone[R any](ctx context.Context, inStream <-chan R) <-chan R {
	outStream := make(chan R)
	if inStream == nil {
		close(outStream)
		panic("OrDone: the provided inStream argument has nil value")
	}

	go func() {
		defer close(outStream)
		for {
			select {
			case <-ctx.Done():
				return
			case res, ok := <-inStream:
				if !ok {
					return
				}
				select {
				case outStream <- res:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return outStream
}
