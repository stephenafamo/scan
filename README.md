# Scan

[![Test Status](https://github.com/stephenafamo/scan/actions/workflows/test.yml/badge.svg)](https://github.com/stephenafamo/scan/actions/workflows/test.yml)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/stephenafamo/scan)
[![Go Reference](https://pkg.go.dev/badge/github.com/stephenafamo/scan.svg)](https://pkg.go.dev/github.com/stephenafamo/scan)
[![Go Report Card](https://goreportcard.com/badge/github.com/stephenafamo/scan)](https://goreportcard.com/report/github.com/stephenafamo/scan)
![GitHub tag (latest SemVer)](https://img.shields.io/github/v/tag/stephenafamo/scan)
[![Coverage Status](https://coveralls.io/repos/github/stephenafamo/scan/badge.svg)](https://coveralls.io/github/stephenafamo/scan)

Scan provides the ability to use database/sql/rows to scan datasets directly to any defined structure.

## Reference

- Standard library scan package. For use with `database/sql`. [Link](https://pkg.go.dev/github.com/stephenafamo/scan/stdscan)
- PGX library scan package. For use with `github.com/jackc/pgx/v5`. [Link](https://pkg.go.dev/github.com/stephenafamo/scan/pgxscan)
- Base scan package. For use with any implementation of [`scan.Queryer`](https://pkg.go.dev/github.com/stephenafamo/scan#Queryer). [Link](https://pkg.go.dev/github.com/stephenafamo/scan)

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
}

func collectIDandEmail(_ context.Context, c cols) any {
    return func(v *Values) (int, string, error) {
        return Value[int](v, "id"), Value[string](v, "email"), nil
    }
}
```

And many more!!

## Using with [pgx](https://github.com/jackc/pgx)

```go
ctx := context.Background()
db, _ := pgxpool.New(ctx, "example-connection-url")

// []User{...}
users, _ := pgxscan.All(ctx, db, scan.StructMapper[User](), `SELECT id, name, email, age FROM users`)
```

## Using with other DB packages

Instead of `github.com/stephenafamo/scan/stdscan`, use the base package `github.com/stephenafam/scan` which only needs an executor that implements the right interface.  
Both `stdscan` and `pgxscan` are based on this.

## How it works

### Scanning Functions

#### `One()`

Use `One()` to scan and return **a single** row.

```go
// User{...}
user, _ := stdscan.One(ctx, db, scan.StructMapper[User](), `SELECT id, name, email, age FROM users`)
```

#### `All()`

Use `All()` to scan and return **all** rows.

```go
// []User{...}
users, _ := stdscan.All(ctx, db, scan.StructMapper[User](), `SELECT id, name, email, age FROM users`)
```

#### `Each()`

Use `Each()` to iterate over the rows of a query using range.

It works with the [range-over-func](https://tip.golang.org/blog/range-functions) syntax.

```go
// []User{...}
for user, err := range scan.Each(ctx, db, scan.StructMapper[User](), `SELECT id, name, email, age FROM users`) {
    if err != nil {
        return err
    }
    // do something with user
}
```

#### `Cursor()`

Use `Cursor()` to scan each row on demand. This is useful when retrieving large results.

```go
c, _ := stdscan.Cursor(ctx, db, scan.StructMapper[User](), `SELECT id, name, email, age FROM users`)
defer c.Close()

for c.Next() {
    // User{...}
    user := c.Get()
}
```

### Mappers

Each of these functions takes a `Mapper` to indicate how each row should be scanned.  
The `Mapper` has the signature:

```go
type Mapper[T any] func(context.Context, cols) (before BeforeFunc, after func(any) (T, error))

type BeforeFunc = func(*Row) (link any, err error)
```

A mapper returns 2 functions

- **before**: This is called before scanning the row. The mapper should schedule scans using one of the the `ScheduleScan` methods of the `Row`. The return value of the **before** function is passed to the **after** function after scanning values from the database.
- **after**: This is called after the scan operation. The mapper should then covert the link value back to the desired concrete type.

There are some builtin mappers for common cases:

#### `ColumnMapper[T any](name string)`

Maps the value of a single column to the given type. The name of the column must be specified

```go
// []string{"user1@example.com", "user2@example.com", "user3@example.com", ...}
emails, _ := stdscan.All(ctx, db, scan.ColumnMapper[string]("email"), `SELECT id, name, email FROM users`)
```

#### `SingleColumnMapper[T any]`

For queries that return only one column. Since only one column is returned, there is no need to specify the column name.  
This is why it throws an error if the query returns more than one column.

```go
// []string{"user1@example.com", "user2@example.com", "user3@example.com", ...}
emails, _ := stdscan.All(ctx, db, scan.SingleColumnMapper[string], `SELECT email FROM users`)
```

#### `SliceMapper[T any]`

Maps a row into a slice of values `[]T`. Unless all the columns are of the same type, it will likely be used to map the row to `[]any`.

```go
// [][]any{
//    []any{1, "John Doe", "john@example.com"},
//    []any{2, "Jane Doe", "jane@example.com"},
//    ...
// }
users, _ := stdscan.All(ctx, db, scan.SliceMapper[any], `SELECT id, name, email FROM users`)
```

#### `MapMapper[T any]`

Maps a row into a map of values `map[string]T`. The key of the map is the column names. Unless all columns are of the same type, it will likely be used to map to `map[string]any`.

```go
// []map[string]any{
//    map[string]any{"id": 1, "name": John Doe", "email": "john@example.com"},
//    map[string]any{"id": 2, "name": Jane Doe", "email": "jane@example.com"},
//    ...
// }
users, _ := stdscan.All(ctx, db, scan.MapMapper[any], `SELECT id, name, email FROM users`)
```

#### `StructMapper[T any](...MappingOption)`

This is the most advanced mapper. Scans column values into the fields of the struct.

```go
type User struct {
    ID    int    `db:"id"`
    Name  string `db:"name"`
    Email string `db:"email"`
    Age   int    `db:"age"`
}

// []User{...}
users, _ := stdscan.All(ctx, db, scan.StructMapper[User](), `SELECT id, name, email, age FROM users`)
```

The default behaviour of `StructMapper` is often good enough. For more advanced use cases, some options can be passed to the StructMapper.

- **WithStructTagPrefix**: Use this when every column from the database has a prefix.

  ```go
  users, _ := stdscan.All(ctx, db, scan.StructMapper[User](scan.WithStructTagPrefix("user-")),
      `SELECT id AS "user-id", name AS "user-name" FROM users`,
  )
  ```

- **WithRowValidator**: If the `StructMapper` has a row validator, the values will be sent to it before scanning. If the row is invalid (i.e. it returns false), then scanning is skipped and the zero value of the row-type is returned.

- **WithTypeConverter**: If the `StructMapper` has a type converter, all fields of the struct are converted to a new type using the `ConverType` method. After scanning, the values are restored into the struct using the `OriginalValue` method.

#### `CustomStructMapper[T any](MapperSource, ...MappingSourceOption)`

Uses a custom struct maping source which should have been created with [NewStructMapperSource](https://pkg.go.dev/github.com/stephenafamo/scan#NewStructMapperSource).

This works the same way as `StructMapper`, but instead of using the default mapping source, it uses a custom one.

In the example below, we want to use a `scan` as the struct tag key instead of `db`

```go
type User struct {
    ID    int    `scan:"id"`
    Name  string `scan:"name"`
    Email string `scan:"email"`
    Age   int    `scan:"age"`
}

src, _ := NewStructMapperSource(scan.WithStructTagKey("scan"))
// []User{...}
users, _ := stdscan.All(ctx, db, scan.StructMapper[User](), `SELECT id, name, email, age FROM users`)
```

These are the options that can be passed to `NewStructMapperSource`:

- **WithStructTagKey**: Change the struct tag used to map columns to struct fields. Default: **db**
- **WithColumnSeparator**: Change the separator for column names of nested struct fields. Default: **.**
- **WithFieldNameMapper**: Change how Struct field names are mapped to column names when there are no struct tags. Default: **snake_case** (i.e. `CreatedAt` is mapped to `created_at`).
- **WithScannableTypes**: Pass a list of interfaces that if implemented, can be scanned by the executor. This means that a field with this type is treated as a single value and will not check the nested fields. Default: `*sql.Scanner`.
