package scan

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

type (
	cols    = map[string]int
	visited map[reflect.Type]int
)

func (v visited) copy() visited {
	v2 := make(visited, len(v))
	for t, c := range v {
		v2[t] = c
	}

	return v2
}

type mapping = map[string]mapinfo

type mapinfo struct {
	position  []int
	init      [][]int
	isPointer bool
}

// Mapper is a function that return the mapping function.
// Any expensive operation, like reflection should be done outside the returned
// function.
// It is called with the columns from the query to get the mapping function
// which is then used to map every row.
//
// The generator function does not return an error itself to make it less cumbersome
// It is recommended to instead return a mapping function that returns an error
// the ErrorMapper is provider for this
//
// The returned function is called once with values.IsRecording() == true to
// record the expected types
// For each row, the DB values are then scanned into Values before
// calling the returned function.
type Mapper[T any] func(context.Context, cols) func(*Values) (T, error)

// Mappable is an interface of a type that can map its own values
// if a struct implement IMapper, using [StructMapper] to map its values
// will use the MapValues method instead
type Mappable[T any] interface {
	MapValues(cols) func(context.Context, *Values) (T, error)
}

// The generator function does not return an error itself to make it less cumbersome
// so we return a function that only returns an error instead
// This function makes it easy to return this error
func errorMapper[T any](err error, meta ...string) func(*Values) (T, error) {
	err = createError(err, meta...)

	return func(*Values) (T, error) {
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
	return m.cause.Error()
}

// Equal makes it easy to compare mapping errors
func (m *MappingError) Equal(err error) bool {
	var m2 *MappingError
	if !errors.As(err, &m2) {
		return errors.Is(m, err) || errors.Is(err, m)
	}

	if len(m.meta) != len(m2.meta) {
		return false
	}

	// if no meta, the error strings should match exactly
	if len(m.meta) == 0 {
		return m.Error() == m2.Error()
	}

	for k := range m.meta {
		if m.meta[k] != m2.meta[k] {
			return false
		}
	}

	return true
}

// For queries that return only one column
// throws an error if there is more than one column
func SingleColumnMapper[T any](ctx context.Context, c cols) func(*Values) (T, error) {
	if len(c) != 1 {
		err := fmt.Errorf("Expected 1 column but got %d columns", len(c))
		return errorMapper[T](err, "wrong column count", "1", strconv.Itoa(len(c)))
	}

	// Get the column name
	var colName string
	for name := range c {
		colName = name
	}

	return func(v *Values) (T, error) {
		return Value[T](v, colName), nil
	}
}

// Map a column by name.
func ColumnMapper[T any](name string) func(context.Context, cols) func(*Values) (T, error) {
	return func(ctx context.Context, c cols) func(v *Values) (T, error) {
		return func(v *Values) (T, error) {
			return Value[T](v, name), nil
		}
	}
}

// Maps each row into []any in the order
func SliceMapper[T any](ctx context.Context, c cols) func(*Values) ([]T, error) {
	return func(v *Values) ([]T, error) {
		row := make([]T, len(c))

		for name, index := range c {
			row[index] = Value[T](v, name)
		}

		return row, nil
	}
}

// Maps all rows into map[string]T
// Most likely used with interface{} to get a map[string]interface{}
func MapMapper[T any](ctx context.Context, c cols) func(*Values) (map[string]T, error) {
	return func(v *Values) (map[string]T, error) {
		row := make(map[string]T, len(c))

		for name := range c {
			row[name] = Value[T](v, name)
		}

		return row, nil
	}
}
