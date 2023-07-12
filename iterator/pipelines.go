package iterator

import (
	"context"
	"database/sql"
)

func genDataChansFromRows(ctx context.Context, rows *sql.Rows, bufferSize int) <-chan dbResult {
	outStream := make(chan dbResult, bufferSize)

	go func() {
		defer close(outStream)

		select {
		case <-ctx.Done():
		default:
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
		}
	}()

	return outStream
}

type workerFunc[Res any] func(ctx context.Context, in dbResult) Res

func fanOut[O any](ctx context.Context, inStream <-chan dbResult, maxProcs int, worker workerFunc[O]) <-chan <-chan O {
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
			case chanStream <- workerThread(ctx, inStream, worker):
			}
		}

	}()

	return chanStream
}

func workerThread[Out any](ctx context.Context, inStream <-chan dbResult, workerFunc workerFunc[Out]) <-chan Out {
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

func fanIn[T any](ctx context.Context, chanStream <-chan (<-chan T)) chan T {
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
			for t := range orDone(ctx, possStream) {
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

func orDone[R any](ctx context.Context, inStream <-chan R) <-chan R {
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
