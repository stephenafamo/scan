# Scan

Scan provides the ability to use database/sql/rows to scan datasets directly to any defined structure.

## Reference

* Standard library scan package. For use with `database/sql`. [Link](https://pkg.go.dev/github.com/stephenafamo/scan/stdscan)
* Base scan package. For use with any implementation of [`scan.Queryer`](https://pkg.go.dev/github.com/stephenafamo/scan#Queryer). [Link](https://pkg.go.dev/github.com/stephenafamo/scan)

## Using with `database/sql`

```go
package main

import (
    "context"
    "database/sql"

    "github.com/stephenafamo/scan"
    "github.com/stephenafamo/scan/stdscan"
)

type User struct {
    ID    int
    Name  string
    Email string
    Age   int
}

func main() {
    ctx := context.Background()
    db, _ := sql.Open("postgres", "example-connection-url")

    // count: 5 
    count, _ := stdscan.One(ctx, db, scan.SingleColumnMapper[int], "SELECT COUNT(*) FROM users")
    // []int{1, 2, 3, 4, 5}
    userIDs, _ := stdscan.All(ctx, db, scan.SingleColumnMapper[int], "SELECT id FROM users")
    // []User{...}
    users, _ := stdscan.All(ctx, db, scan.StructMapper[User](), `SELECT id, name, email, age FROM users`)

    // []any{
    //     []int{1, 2, 3, 4, 5},
    //     []string{"user1@example.com", "user2@example.com", ...},
    // }
    idsAndEmail, _ := stdscan.Collect(ctx, db, collectIDandEmail, `SELECT id, email FROM users`)
}

func collectIDandEmail(_ context.Context, c cols) any {
    return func(v *Values) (int, string, error) {
        return Value[int](v, "id"), Value[string](v, "email"), nil
    }
}
```

And many more!!

## Using with other DB packages

Instead of `github.com/stephenafamo/scan/stdscan`, use the base package `github.com/stephenafam/scan` which only needs an executor that implements the right interface.

## How it works

Scan take a `Mapper` to indicate how each row should be scanned. After which, `One()` will scan a single row, and `All()` will scan all rows.  
The `Mapper` has the signature:

```go
type Mapper[T any] func(context.Context, cols) func(*Values) (T, error)
```

Any function that has this signature can be used as a `Mapper`. There are some helper types for common cases:

* `ColumnMapper[T any](name string)`: Maps the value of a single column by name.
* `SingleColumnMapper[T any]`: For queries that return only one column. Throws an error if the query returns more than one column.
* `SliceMapper[T any]`: Maps a row into a slice of values `[]T`. Unless all the columns are of the same type, it will likely be used to map the row to `[]any`.
* `MapMapper[T any]`: Maps a row into a map of values `map[string]T`. The key of the map is the column names. Unless all columns are of the same type, it will likely be used to map to `map[string]any`.
* `StructMapper[T any](...MappingOption)`: This is the most advanced mapper. Scans column values into the fields of the struct.
* `CustomStructMapper[T any](MapperSource, ...MappingOption)`: Uses a custom struct maping source which should have been created with [NewStructMapperSource](https://pkg.go.dev/github.com/stephenafamo/scan#NewStructMapperSource).
