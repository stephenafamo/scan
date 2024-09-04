package scan

import (
	"context"
	"database/sql"
)

// One scans a single row from the query and maps it to T using a [Queryer]
func One[T any](ctx context.Context, exec Queryer, m Mapper[T], query string, args ...any) (T, error) {
	var t T

	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return t, err
	}
	defer rows.Close()

	return OneFromRows(ctx, m, rows)
}

// OneFromRows scans a single row from the given [Rows] result and maps it to T using a [Queryer]
func OneFromRows[T any](ctx context.Context, m Mapper[T], rows Rows) (T, error) {
	var t T

	allowUnknown, _ := ctx.Value(CtxKeyAllowUnknownColumns).(bool)
	v, err := wrapRows(rows, allowUnknown)
	if err != nil {
		return t, err
	}

	before, after := m(ctx, v.columnsCopy())

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return t, err
		}
		return t, sql.ErrNoRows
	}

	t, err = scanOneRow(v, before, after)
	if err != nil {
		return t, err
	}

	return t, rows.Err()
}

// All scans all rows from the query and returns a slice []T of all rows using a [Queryer]
func All[T any](ctx context.Context, exec Queryer, m Mapper[T], query string, args ...any) ([]T, error) {
	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return AllFromRows(ctx, m, rows)
}

// AllFromRows scans all rows from the given [Rows] and returns a slice []T of all rows using a [Queryer]
func AllFromRows[T any](ctx context.Context, m Mapper[T], rows Rows) ([]T, error) {
	allowUnknown, _ := ctx.Value(CtxKeyAllowUnknownColumns).(bool)
	v, err := wrapRows(rows, allowUnknown)
	if err != nil {
		return nil, err
	}

	before, after := m(ctx, v.columnsCopy())

	var results []T
	for rows.Next() {
		one, err := scanOneRow(v, before, after)
		if err != nil {
			return nil, err
		}

		results = append(results, one)
	}

	return results, rows.Err()
}

// Cursor runs a query and returns a cursor that works similar to *sql.Rows
func Cursor[T any](ctx context.Context, exec Queryer, m Mapper[T], query string, args ...any) (ICursor[T], error) {
	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return CursorFromRows(ctx, m, rows)
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
func Each[T any](ctx context.Context, exec Queryer, m Mapper[T], query string, args ...any) func(func(T, error) bool) {
	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return func(yield func(T, error) bool) { yield(*new(T), err) }
	}

	allowUnknown, _ := ctx.Value(CtxKeyAllowUnknownColumns).(bool)
	wrapped, err := wrapRows(rows, allowUnknown)
	if err != nil {
		rows.Close()
		return func(yield func(T, error) bool) { yield(*new(T), err) }
	}

	before, after := m(ctx, wrapped.columnsCopy())

	return func(yield func(T, error) bool) {
		defer rows.Close()

		for rows.Next() {
			val, err := scanOneRow(wrapped, before, after)
			if !yield(val, err) {
				return
			}
		}
	}
}

// CursorFromRows returns a cursor from [Rows] that works similar to *sql.Rows
func CursorFromRows[T any](ctx context.Context, m Mapper[T], rows Rows) (ICursor[T], error) {
	allowUnknown, _ := ctx.Value(CtxKeyAllowUnknownColumns).(bool)
	v, err := wrapRows(rows, allowUnknown)
	if err != nil {
		return nil, err
	}

	before, after := m(ctx, v.columnsCopy())

	return &cursor[T]{
		v:      v,
		before: before,
		after:  after,
	}, nil
}

func scanOneRow[T any](v *Row, before func(*Row) (any, error), after func(any) (T, error)) (T, error) {
	val, err := before(v)
	if err != nil {
		var t T
		return t, err
	}

	err = v.scanCurrentRow()
	if err != nil {
		var t T
		return t, err
	}

	return after(val)
}
