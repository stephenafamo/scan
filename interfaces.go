package scan

import (
	"context"
)

// Queryer is the main interface used in this package
// it is expected to run the query and args and return a set of Rows
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (Rows, error)
}

// Rows is an interface that is expected to be returned as the result of a query
type Rows interface {
	Scan(...any) error
	Columns() ([]string, error)
	Next() bool
	Close() error
	Err() error
}
