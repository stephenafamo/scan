package scan

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
)

type (
	cols    = []string
	visited map[reflect.Type]int
)

func (v visited) copy() visited {
	v2 := make(visited, len(v))
	for t, c := range v {
		v2[t] = c
	}

	return v2
}

type mapinfo struct {
	name      string
	position  []int
	init      [][]int
	isPointer bool
}

type mapping []mapinfo

func (m mapping) cols() []string {
	cols := make([]string, len(m))
	for i, info := range m {
		cols[i] = info.name
	}

	return cols
}

// Mapper is a function that return the mapping functions.
// Any expensive operation, like reflection should be done outside the returned
// function.
// It is called with the columns from the query to get the mapping functions
// which is then used to map every row.
//
// The Mapper does not return an error itself to make it less cumbersome
// It is recommended to instead return a function that returns an error
// the [ErrorMapper] is provider for this
type Mapper[T any] func(context.Context, cols) (before BeforeFunc, after func(any) (T, error))

// BeforeFunc is returned by a mapper and is called before a row is scanned
// Scans should be scheduled with either
// the [*Row.ScheduleScan] or [*Row.ScheduleScanx] methods
type BeforeFunc = func(*Row) (link any, err error)

// The generator function does not return an error itself to make it less cumbersome
// so we return a function that only returns an error instead
// This function makes it easy to return this error
func ErrorMapper[T any](err error, meta ...string) (func(*Row) (any, error), func(any) (T, error)) {
	err = createError(err, meta...)

	return func(v *Row) (any, error) {
			return nil, err
		}, func(any) (T, error) {
			var t T
			return t, err
		}
}

// Returns a [MappingError] with some optional metadata
func createError(err error, meta ...string) error {
	if me, ok := err.(*MappingError); ok && len(meta) == 0 {
		return me
	}

	return &MappingError{cause: err, meta: meta}
}

// MappingError wraps another error and holds some additional metadata
type MappingError struct {
	meta  []string // easy compare
	cause error
}

// Unwrap returns the wrapped error
func (m *MappingError) Unwrap() error {
	return m.cause
}

// Error implements the error interface
func (m *MappingError) Error() string {
	if m.cause == nil {
		return ""
	}

	return m.cause.Error()
}

// For queries that return only one column
// throws an error if there is more than one column
func SingleColumnMapper[T any](ctx context.Context, c cols) (before func(*Row) (any, error), after func(any) (T, error)) {
	if len(c) != 1 {
		err := fmt.Errorf("Expected 1 column but got %d columns", len(c))
		return ErrorMapper[T](err, "wrong column count", "1", strconv.Itoa(len(c)))
	}

	return func(v *Row) (any, error) {
			var t T
			v.ScheduleScan(c[0], &t)
			return &t, nil
		}, func(v any) (T, error) {
			return *(v.(*T)), nil
		}
}

// Map a column by name.
func ColumnMapper[T any](name string) func(ctx context.Context, c cols) (before func(*Row) (any, error), after func(any) (T, error)) {
	return func(ctx context.Context, c cols) (before func(*Row) (any, error), after func(any) (T, error)) {
		return func(v *Row) (any, error) {
				var t T
				v.ScheduleScan(name, &t)
				return &t, nil
			}, func(v any) (T, error) {
				return *(v.(*T)), nil
			}
	}
}

// Maps each row into []any in the order
func SliceMapper[T any](ctx context.Context, c cols) (before func(*Row) (any, error), after func(any) ([]T, error)) {
	return func(v *Row) (any, error) {
			row := make([]T, len(c))

			for index, name := range c {
				v.ScheduleScan(name, &row[index])
			}

			return row, nil
		}, func(v any) ([]T, error) {
			return v.([]T), nil
		}
}

// Maps all rows into map[string]T
// Most likely used with interface{} to get a map[string]interface{}
func MapMapper[T any](ctx context.Context, c cols) (before func(*Row) (any, error), after func(any) (map[string]T, error)) {
	return func(v *Row) (any, error) {
			row := make([]*T, len(c))

			for index, name := range c {
				var t T
				v.ScheduleScan(name, &t)
				row[index] = &t
			}

			return row, nil
		}, func(v any) (map[string]T, error) {
			row := make(map[string]T, len(c))
			slice := v.([]*T)
			for index, name := range c {
				row[name] = *slice[index]
			}

			return row, nil
		}
}
