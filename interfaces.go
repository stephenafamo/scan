package scan

import (
	"context"
)

// Queryer is the main interface used in this package
// it is expected to run the query and args and return a set of Rows
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (Rows, error)
}

// Row represents a single row in the Rows results
// can be scanned into a number of given values
type Row interface {
	Scan(...any) error
}

// Rows is an interface that is expected to be returned as the result of a query
type Rows interface {
	Row
	Columns() ([]string, error)
	Next() bool
	Close() error
	Err() error
}
