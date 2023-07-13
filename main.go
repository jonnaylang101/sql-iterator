package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jonnaylang101/sql-iterator/database"
	"github.com/jonnaylang101/sql-iterator/iterator"
)

var (
	db    database.Database[DbResult]
	table string
)

type DbResult struct {
	Name string
	Age  int
	Err  error
}

type SentenceResult struct {
	Sentence string
	Err      error
}

func init() {
	cfg := database.DbConfig{}
	var err error
	cfg.Host, err = Mustenv("HOST")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	cfg.Port, err = Mustenv("PORT")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	cfg.DbName, err = Mustenv("DB_NAME")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	cfg.User, err = Mustenv("USER")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	cfg.Password, err = Mustenv("PASSWORD")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	table, err = Mustenv("TABLE")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	db, err = database.NewDB(cfg, DbResultBinder, 20)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	itr := iterator.New[DbResult, SentenceResult](db, table)

	query := `SELECT firstname, age FROM ` + table
	sentences, err := itr.Iterate(ctx, query, SentenceWorker)
	if err != nil {
		return err
	}

	for _, sen := range sentences {
		if sen.Err != nil {
			return sen.Err
		}

		fmt.Println(sen.Sentence)
	}

	return nil
}

func Mustenv(ev string) (string, error) {
	t := os.Getenv(ev)
	if t == "" {
		return t, fmt.Errorf("missing env var = %s", ev)
	}

	return t, nil
}

var DbResultBinder database.CustomBinder[DbResult] = func(rows *sql.Rows) DbResult {
	d := DbResult{}
	d.Err = rows.Scan(&d.Name, &d.Age)

	return d
}

var SentenceWorker iterator.WorkerFunc[DbResult, SentenceResult] = func(ctx context.Context, in DbResult) SentenceResult {
	time.Sleep(time.Millisecond * 500) // emulate a longer running process
	return SentenceResult{
		Err:      in.Err,
		Sentence: fmt.Sprintf("This is %s, they are %d years old", in.Name, in.Age),
	}
}
