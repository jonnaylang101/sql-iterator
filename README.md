# SQL Iterator

This is initially a rough sketch of a concurren, generic MySQL database Iterator.

## In use
For example, if we wanted to perform the simple task of getting all names from a column in a table of people before reversing those names and printing them out to the console...

```golang

// set your type to bind db rows to (ensure you add an error field as all errors are handled at the end of the request)
type rowData struct {
    Name string
    Err error
}

// set your type to return the results in (ensure you add an error field for the same reasons as above)
type resData struct {
    RevName string
    Err error
}

func main() {
    cfg := database.DbConfig{
        Host: "<your_host>",
        Port: "<your_port>",
        DbName: "<your_db_name>",
        User: "<your_username>",
        Password: "<your_password>",
    }

    db, err = database.New(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

    table := "<your_table>"

    // init the iterator with the types you wish to use
    itr := iterator.New[rowData, resData](db, table)

    // create a query that works with your CustomBinder function by matching columns to fields in the order you
    // want to scan them
    query := `SELECT name FROM ` + table

    // perform the iteration
	revNames, err := itr.Iterate(ctx, query, rowDataBinder, nameReverser, setMaxBufferSize(20))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

    // print the results
    for _, rn := range revNames {
        if rn.Err != nil {
            fmt.Println(err) // handle the errors however you see fit
            continue
        }
        fmt.Println(rn.RevName)
    }
}

// setup a custom binder to bind data from db rows into your chosen object (rowData in this case)
var rowDataBinder iterator.CustomBinder[rowData] = func(rows *sql.Rows) rowData {
	rd := rowData{}
	rd.Err = rows.Scan(&rd.Name)

	return rd
}

var nameReverser iterator.WorkerFunc[rowData, resData] = func(ctx context.Context, in rowData) resData {
	time.Sleep(time.Millisecond * 500) // emulate a longer running process
	return resData{
		Err:      in.Err,
        RevName:  reverseString(in.Name), // this method is out of scope for this documentation
	}
}

```