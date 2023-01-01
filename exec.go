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
	v, err := newValues(rows, allowUnknown)
	if err != nil {
		return t, err
	}

	before, after := m(ctx, v.columnsCopy())

	// Record the mapping
	v.startRecording()
	if before != nil {
		if err = before(v); err != nil {
			return t, err
		}
	}
	if _, err = after(v); err != nil {
		return t, err
	}
	if err := v.stopRecording(); err != nil {
		return t, err
	}

	if !rows.Next() {
		return t, sql.ErrNoRows
	}

	t, err = scanOneRow(v, rows, before, after)
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
	v, err := newValues(rows, allowUnknown)
	if err != nil {
		return nil, err
	}

	before, after := m(ctx, v.columnsCopy())

	// Record the mapping
	v.startRecording()
	if before != nil {
		if err = before(v); err != nil {
			return nil, err
		}
	}
	if _, err = after(v); err != nil {
		return nil, err
	}
	if err := v.stopRecording(); err != nil {
		return nil, err
	}

	var results []T
	for rows.Next() {
		one, err := scanOneRow(v, rows, before, after)
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

// CursorFromRows returns a cursor from [Rows] that works similar to *sql.Rows
func CursorFromRows[T any](ctx context.Context, m Mapper[T], rows Rows) (ICursor[T], error) {
	allowUnknown, _ := ctx.Value(CtxKeyAllowUnknownColumns).(bool)
	v, err := newValues(rows, allowUnknown)
	if err != nil {
		return nil, err
	}

	before, after := m(ctx, v.columnsCopy())

	// Record the mapping
	v.startRecording()
	if before != nil {
		if err = before(v); err != nil {
			return nil, err
		}
	}
	if _, err = after(v); err != nil {
		return nil, err
	}
	if err := v.stopRecording(); err != nil {
		return nil, err
	}

	return &cursor[T]{
		r:      rows,
		v:      v,
		before: before,
		after:  after,
	}, nil
}

func scanOneRow[T any](v *Values, rows Rows, before func(*Values) error, after func(*Values) (T, error)) (T, error) {
	if before != nil {
		if err := before(v); err != nil {
			var t T
			return t, err
		}
	}

	err := v.scanRow(rows)
	if err != nil {
		var t T
		return t, err
	}

	return after(v)
}
