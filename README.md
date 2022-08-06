# Scan

Scan provides the ability to use database/sql/rows to scan datasets directly to any defined structure.

```go
package main

import (
	"context"
	"database/sql"

	"github.com/stephenafamo/scan"
	"github.com/stephenafamo/scan/stdscan"
)

type User struct {
	ID    string
	Name  string
	Email string
	Age   int
}

func main() {
	ctx := context.Background()
	db, _ := sql.Open("postgres", "example-connection-url")

	users, _ := stdscan.All[User](ctx, db, scan.StructMapper[User], `SELECT id, name, email, age FROM users`)
    // users is a []User that contains all rows
}
```
