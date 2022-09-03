package scan

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
)

// One scans a single row from the query and maps it to T using a [Queryer]
func One[T any](ctx context.Context, exec Queryer, m Mapper[T], query string, args ...any) (T, error) {
	var t T

	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return t, err
	}
	defer rows.Close()

	allowUnknown, _ := ctx.Value(CtxKeyAllowUnknownColumns).(bool)
	v, err := newValues(rows, allowUnknown)
	if err != nil {
		return t, err
	}

	genFunc := m(ctx, v.columnsCopy())

	// Record the mapping
	v.startRecording()
	if _, err = genFunc(v); err != nil {
		return t, err
	}
	if err := v.stopRecording(); err != nil {
		return t, err
	}

	if !rows.Next() {
		return t, sql.ErrNoRows
	}

	if err = v.scanRow(rows); err != nil {
		return t, err
	}

	t, err = genFunc(v)
	if err != nil {
		return t, err
	}

	return t, rows.Err()
}

// All scans all rows from the query and returns a slice []T of all rows using a [Queryer]
func All[T any](ctx context.Context, exec Queryer, m Mapper[T], query string, args ...any) ([]T, error) {
	var results []T

	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	allowUnknown, _ := ctx.Value(CtxKeyAllowUnknownColumns).(bool)
	v, err := newValues(rows, allowUnknown)
	if err != nil {
		return nil, err
	}

	genFunc := m(ctx, v.columnsCopy())

	// Record the mapping
	v.startRecording()
	if _, err = genFunc(v); err != nil {
		return nil, err
	}
	if err := v.stopRecording(); err != nil {
		return nil, err
	}

	for rows.Next() {
		err = v.scanRow(rows)
		if err != nil {
			return nil, err
		}

		one, err := genFunc(v)
		if err != nil {
			return nil, err
		}

		results = append(results, one)
	}

	return results, rows.Err()
}

// Cursor returns a cursor that works similar to *sql.Rows
func Cursor[T any](ctx context.Context, exec Queryer, m Mapper[T], query string, args ...any) (ICursor[T], error) {
	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	allowUnknown, _ := ctx.Value(CtxKeyAllowUnknownColumns).(bool)
	v, err := newValues(rows, allowUnknown)
	if err != nil {
		return nil, err
	}

	genFunc := m(ctx, v.columnsCopy())

	// Record the mapping
	v.startRecording()
	if _, err = genFunc(v); err != nil {
		return nil, err
	}
	if err := v.stopRecording(); err != nil {
		return nil, err
	}

	return &cursor[T]{
		r: rows,
		v: v,
		f: genFunc,
	}, nil
}

var (
	ErrBadCollectorReturn   = errors.New("collector does not return a function")
	ErrBadCollectFuncInput  = errors.New("collect func must only take *Values as input")
	ErrBadCollectFuncOutput = errors.New("collect func must return at least 2 values with the last being an error")
)

// Collect multiple slices of values from a single query
// collector must be of the structure
// func(context.Context, map[string]int) func(*Values) (t1, t2, ..., error)
// The returned slice contains values like this
// {[]t1, []t2}
func Collect(ctx context.Context, exec Queryer, collector func(context.Context, cols) any, query string, args ...any) ([]any, error) {
	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	allowUnknown, _ := ctx.Value(CtxKeyAllowUnknownColumns).(bool)
	v, err := newValues(rows, allowUnknown)
	if err != nil {
		return nil, err
	}
	vref := reflect.ValueOf(v)

	genFunc := reflect.ValueOf(collector(ctx, v.columnsCopy()))

	if genFunc.Kind() != reflect.Func {
		return nil, ErrBadCollectorReturn
	}

	genFuncTyp := genFunc.Type()
	if genFuncTyp.NumIn() != 1 || genFuncTyp.In(0) != vref.Type() {
		return nil, ErrBadCollectFuncInput
	}

	nOut := genFuncTyp.NumOut()
	if nOut < 2 || genFuncTyp.Out(nOut-1) != reflect.TypeOf((*error)(nil)).Elem() {
		return nil, ErrBadCollectFuncOutput
	}

	resultTyp := make([]reflect.Type, nOut-1)
	results := make([]reflect.Value, nOut-1)
	for i := 0; i < nOut-1; i++ {
		sliceTyp := reflect.SliceOf(genFuncTyp.Out(i))
		resultTyp[i] = sliceTyp
		results[i] = reflect.New(sliceTyp).Elem()
	}

	// Record the mapping
	v.startRecording()
	out := genFunc.Call([]reflect.Value{vref})
	errI := out[len(out)-1].Interface() // if it is just nil, it may panic
	if err != nil {
		return nil, errI.(error)
	}
	if err := v.stopRecording(); err != nil {
		return nil, err
	}

	for rows.Next() {
		err = v.scanRow(rows)
		if err != nil {
			return nil, err
		}

		out := genFunc.Call([]reflect.Value{vref})
		errI = out[len(out)-1].Interface()
		if err != nil {
			return nil, errI.(error)
		}

		for i := 0; i < nOut-1; i++ {
			results[i] = reflect.Append(results[i], out[i])
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	resp := make([]any, len(results))
	for i, v := range results {
		resp[i] = v.Interface()
	}

	return resp, nil
}
