package scan

import (
	"fmt"
	"reflect"
)

var zeroValue reflect.Value

func wrapRows(r Rows, allowUnknown bool) (*Row, error) {
	cols, err := r.Columns()
	if err != nil {
		return nil, err
	}

	return &Row{
		r:                r,
		columns:          cols,
		scanDestinations: make([]reflect.Value, len(cols)),
	}, nil
}

// Row represents a single row from the query and is passed to the [BeforeFunc]
// when sent to a mapper's before function, scans should be scheduled
// with either the [ScheduleScan] or [ScheduleScanx] methods
type Row struct {
	r                   Rows
	columns             []string
	scanDestinations    []reflect.Value
	unknownDestinations []string
	allowUnknown        bool
}

func (r *Row) ScheduleScan(colName string, val any) {
	r.ScheduleScanx(colName, reflect.ValueOf(val))
}

func (r *Row) ScheduleScanx(colName string, val reflect.Value) {
	colIndex := r.index(colName)
	if colIndex == -1 {
		r.unknownDestinations = append(r.unknownDestinations, colName)
		return
	}

	r.scanDestinations[colIndex] = val
}

func (r *Row) index(name string) int {
	for i, n := range r.columns {
		if name == n {
			return i
		}
	}

	return -1
}

// To get a copy of the columns to pass to mapper generators
// since modifing the map can have unintended side effects.
// Ideally, a generator should only call this once
func (r *Row) columnsCopy() []string {
	m := make([]string, len(r.columns))
	copy(m, r.columns)
	return m
}

func (r *Row) scanCurrentRow() error {
	if len(r.unknownDestinations) > 0 {
		return createError(fmt.Errorf("unknown columns to map to: %v", r.unknownDestinations), r.unknownDestinations...)
	}

	targets := make([]any, len(r.columns))

	for i, name := range r.columns {
		dest := r.scanDestinations[i]
		if dest != zeroValue {
			targets[i] = dest.Interface()
			continue
		}

		if !r.allowUnknown {
			err := fmt.Errorf("No destination for column %s", name)
			return createError(err, "no destination", name)
		}
	}

	err := r.r.Scan(targets...)
	if err != nil {
		return err
	}

	r.scanDestinations = make([]reflect.Value, len(r.columns))
	return nil
}
