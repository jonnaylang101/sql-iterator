package database

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type DbConfig struct {
	Host, Port, DbName string
	User, Password     string
}

func (cfg DbConfig) CreateConnectionString() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DbName)
}

func New(dbConfig DbConfig) (*sql.DB, error) {
	db, err := sql.Open("mysql", dbConfig.CreateConnectionString())
	if err != nil {
		return db, fmt.Errorf("database.New: error occurred whild initializing the database: %v", err)
	}

	return db, nil
}
