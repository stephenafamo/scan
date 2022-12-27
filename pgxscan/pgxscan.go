package pgxscan

import (
	"context"

	"github.com/jackc/pgx/v5"
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

// Collect multiple slices of values from a single query
// collector must be of the structure
// func(cols) func(*Values) (t1, t2, ..., error)
// The returned slice contains values like this
// {[]t1, []t2}
func Collect(ctx context.Context, exec Queryer, collector func(context.Context, map[string]int) any, sql string, args ...any) ([]any, error) {
	return scan.Collect(ctx, convert(exec), collector, sql, args...)
}

// A Queryer that returns the concrete type [*sql.Rows]
type Queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// convert wraps an Queryer and makes it a Queryer
func convert(wrapped Queryer) scan.Queryer {
	return queryer{wrapped: wrapped}
}

type queryer struct {
	wrapped Queryer
}

type rows struct {
	pgx.Rows
}

func (r rows) Close() error {
	r.Rows.Close()
	return nil
}

func (r rows) Columns() ([]string, error) {
	fields := r.FieldDescriptions()
	cols := make([]string, len(fields))

	for i, field := range fields {
		cols[i] = field.Name
	}

	return cols, nil
}

// QueryContext executes a query that returns rows, typically a SELECT. The args are for any placeholder parameters in the query.
func (q queryer) QueryContext(ctx context.Context, query string, args ...any) (scan.Rows, error) {
	r, err := q.wrapped.Query(ctx, query, args...)
	return rows{r}, err
}
