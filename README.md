# go-postgres

A library that simplifies access to PostgreSQL databases.

#### Some considerations

1. Despite `go-postgres` uses [Jack Christensen's `PGX` library](https://github.com/jackc/pgx) internally,
   it aims to act as a generic database driver like `database/sql` and avoid the developer to use specific
   `PGX` types and routines.
2. Most of the commonly used types in Postgres can be mapped to standard Golang types including `time.Time`
   for timestamps (except Postgres' time with tz which is not supported)
3. When reading `JSON/JSONB` fields, the code will try to unmarshall it into the destination variable. In
   order to just retrieve the json value as a string, add the `::text` suffix to the field in the `SELECT`
   query.
4. To avoid overflows on high `uint64` values, you can store them in `NUMERIC(24,0)` fields.
5. When reading time-only fields, the date part of the `time.Time` variable is set to `January 1, 2000`.

## Usage with example

```golang
package main

import (
	"context"

	"github.com/randlabs/go-postgres"
)

type Data struct {
	Id   int
	Name string
}

func main() {
	ctx := context.Background()

	// Create database driver
	db, err := postgres.New(ctx, postgres.Options{
		Host:     "127.0.0.1",
		Port:     5432,
		User:     "postgres",
		Password: "1234",
		Name:     "sampledb",
	})
	if err != nil {
		// ....
	}
	defer db.Close()

	// Insert some data
	data := Data{
		Id:   1,
		Name: "example",
	}

	_, err = db.Exec(ctx, `INSERT INTO test_table (id, name) VALUES ($1, $2)`, data.Id, data.Name)
	if err != nil {
		// ....
	}

	// Read it
	var name string
	err = db.QueryRow(ctx, `SELECT name FROM test_table WHERE id = $1)`, 1).Scan(&name)
	if err != nil {
		// ....
		if postgres.IsNoRowsError(err) {
			// This should not happen. We cannot find the record we just inserted.
		}
	}

	// ....
}
```

## LICENSE

See [LICENSE](/LICENSE) file for details.
