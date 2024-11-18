package stdscan

import (
	"context"
	"database/sql"

	"github.com/stephenafamo/scan"
)

// One scans a single row from the query and maps it to T using a [StdQueryer]
// this is for use with *sql.DB, *sql.Tx or *sql.Conn or any similar implementations
// that return *sql.Rows
func One[T any](ctx context.Context, exec Queryer, m scan.Mapper[T], sql string, args ...any) (T, error) {
	return scan.One(ctx, convert(exec), m, sql, args...)
}

// All scans all rows from the query and returns a slice []T of all rows using a [StdQueryer] this is for use with *sql.DB, *sql.Tx or *sql.Conn or any similar implementations
// that return *sql.Rows
func All[T any](ctx context.Context, exec Queryer, m scan.Mapper[T], sql string, args ...any) ([]T, error) {
	return scan.All(ctx, convert(exec), m, sql, args...)
}

// Cursor returns a cursor that works similar to *sql.Rows
func Cursor[T any](ctx context.Context, exec Queryer, m scan.Mapper[T], sql string, args ...any) (scan.ICursor[T], error) {
	return scan.Cursor(ctx, convert(exec), m, sql, args...)
}

// Each returns a function that can be used to iterate over the rows of a query
// this function works with range-over-func so it is possible to do
//
//	for val, err := range scan.Each(ctx, exec, m, query, args...) {
//	    if err != nil {
//	        return err
//	    }
//	    // do something with val
//	}
func Each[T any](ctx context.Context, exec Queryer, m scan.Mapper[T], query string, args ...any) func(func(T, error) bool) {
	return scan.Each(ctx, convert(exec), m, query, args...)
}

// A Queryer that returns the concrete type [*sql.Rows]
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// convert wraps an Queryer and makes it a Queryer
func convert(wrapped Queryer) scan.Queryer {
	return queryer{wrapped: wrapped}
}

type queryer struct {
	wrapped Queryer
}

// QueryContext executes a query that returns rows, typically a SELECT. The args are for any placeholder parameters in the query.
func (q queryer) QueryContext(ctx context.Context, query string, args ...any) (scan.Rows, error) {
	return q.wrapped.QueryContext(ctx, query, args...)
}
