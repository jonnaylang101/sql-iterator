package main

import (
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jonnaylang101/sql-iterator/database"
	"github.com/jonnaylang101/sql-iterator/iterator"
)

func main() {
	if err := run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run() error {
	var err error

	cfg := database.DbConfig{}
	cfg.Host, err = Mustenv("HOST")
	if err != nil {
		return err
	}

	cfg.Port, err = Mustenv("PORT")
	if err != nil {
		return err
	}

	cfg.DbName, err = Mustenv("DB_NAME")
	if err != nil {
		return err
	}

	cfg.User, err = Mustenv("USER")
	if err != nil {
		return err
	}

	cfg.Password, err = Mustenv("PASSWORD")
	if err != nil {
		return err
	}

	db, err := database.New(cfg)
	if err != nil {
		return err
	}

	table, err := Mustenv("TABLE")
	if err != nil {
		return err
	}

	sentences, err := iterator.MakeSentencesFromDatabaseRows(db, table)
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
