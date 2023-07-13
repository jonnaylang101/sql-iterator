package database

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jonnaylang101/sql-iterator/iterator"
)

type DbConfig struct {
	Host, Port, DbName string
	User, Password     string
}

func (cfg DbConfig) CreateConnectionString() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DbName)
}

type Database[T any] interface {
	Query(context.Context, string) (<-chan T, error)
}

type database[T any] struct {
	sql          *sql.DB
	bufferSize   int
	customBinder iterator.CustomBinder[T]
}

func NewDB[T any](cfg DbConfig, binder iterator.CustomBinder[T], bufferSize int) (Database[T], error) {
	sql, err := sql.Open("mysql", cfg.CreateConnectionString())
	if err != nil {
		return &database[T]{}, fmt.Errorf("database.New: error occurred whild initializing the database: %v", err)
	}

	return &database[T]{
		sql:          sql,
		bufferSize:   bufferSize,
		customBinder: binder,
	}, nil
}

func (db *database[T]) Query(ctx context.Context, qString string) (<-chan T, error) {
	rows, err := db.sql.Query(qString)
	if err != nil {
		return nil, err
	}

	return genDataChansFromRows(ctx, rows, db.bufferSize, db.customBinder), nil
}

// this is a copy for now until this idea is tested
func genDataChansFromRows[R any](ctx context.Context, rows *sql.Rows, bufferSize int, binder iterator.CustomBinder[R]) <-chan R {
	outStream := make(chan R, bufferSize)

	go func() {
		defer close(outStream)

		select {
		case <-ctx.Done():
		default:
			for rows.Next() {
				outStream <- binder(rows)
			}
		}
	}()

	return outStream
}
