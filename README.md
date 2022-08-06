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
    users, _ := stdscan.All(ctx, db, scan.StructMapper[User], `SELECT id, name, email, age FROM users`)

    // []any{
    //     []int{1, 2, 3, 4, 5},
    //     []string{"user1@example.com", "user2@example.com", ...},
    // }
    idsAndEmail, _ := stdscan.Collect(ctx, db, collectIDandEmail, `SELECT id, email FROM users`)
}

func collectIDandEmail(c cols) any {
    return func(v *Values) (int, string, error) {
        return Value[int](v, "id"), Value[string](v, "email"), nil
    }
}
```

And many more!!

## Using with other DB packages

Instead of `github.com/stephenafamo/scan/stdscan`, use the base package `github.com/stephenafam/scan` which only needs an executor that implements the right interface.

