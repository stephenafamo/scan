package scan

import (
	"context"
	"reflect"
)

type contextKey string

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

type TypeConverter interface {
	// TypeToDestination is called with the expected type of the column
	// it is expected to return a pointer to the desired value to scan into
	// the returned destination is directly scanned into
	TypeToDestination(reflect.Type) reflect.Value

	// ValueFromDestination retrieves the original value from the destination
	// the returned value is set back to the appropriate struct field
	ValueFromDestination(reflect.Value) reflect.Value
}

// RowValidator is called with pointer to all the values from a row
// to determine if the row is valid
// if it is not, the zero type for that row is returned
type RowValidator = func(cols []string, vals []reflect.Value) bool

type StructMapperSource interface {
	getMapping(reflect.Type) (mapping, error)
}
