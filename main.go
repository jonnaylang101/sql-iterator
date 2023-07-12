package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jonnaylang101/sql-iterator/database"
	"github.com/jonnaylang101/sql-iterator/iterator"
)

var (
	db    *sql.DB
	table string
)

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

	db, err = database.New(cfg)
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
	sentences, err := iterator.MakeSentencesFromDatabaseRows(ctx, db, table, setMaxBufferSize(20))
	if err != nil {
		return err
	}

	for _, sen := range sentences {
		fmt.Println(sen)
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

func setMaxBufferSize(s int) iterator.Option {
	return func(io *iterator.IteratorOptions) {
		io.MaxBufferSize = s
	}
}
