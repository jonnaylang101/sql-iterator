package iterator

import (
	"database/sql"
)

// customBinder allows us to bind rows to data structures of our choice
type customBinder[R any] func(rows *sql.Rows) R
