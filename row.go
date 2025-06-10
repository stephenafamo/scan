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
		allowUnknown:     allowUnknown,
	}, nil
}

// Row represents a single row from the query and is passed to the [BeforeFunc]
// when sent to a mapper's before function, scans should be scheduled
// with any of the following ScheduleScan methods
// - [*Row.ScheduleScanByName]
// - [*Row.ScheduleScanByNameX]
// - [*Row.ScheduleScanByIndex]
// - [*Row.ScheduleScanByIndexX]
type Row struct {
	r                   Rows
	columns             []string
	scanDestinations    []reflect.Value
	unknownDestinations []string
	allowUnknown        bool
}

// ScheduleScanByName schedules a scan for the column name into the given value
// val should be a pointer
func (r *Row) ScheduleScanByName(colName string, val any) {
	r.ScheduleScanByNameX(colName, reflect.ValueOf(val))
}

// ScheduleScanByNameX schedules a scan for the column name into the given reflect.Value
// val.Kind() should be reflect.Pointer
func (r *Row) ScheduleScanByNameX(colName string, val reflect.Value) {
	for i, n := range r.columns {
		if n == colName {
			r.scanDestinations[i] = val
			return
		}
	}

	r.unknownDestinations = append(r.unknownDestinations, colName)
}

// ScheduleScanByIndex schedules a scan for the column number into the given value
// val should be a pointer
func (r *Row) ScheduleScanByIndex(colIndex int, val any) {
	r.scanDestinations[colIndex] = reflect.ValueOf(val)
}

// ScheduleScanByIndex schedules a scan for the column number into the given reflect.Value
// val should be a pointer
func (r *Row) ScheduleScanByIndexX(colIndex int, val reflect.Value) {
	r.scanDestinations[colIndex] = val
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

	targets, err := r.createTargets()
	if err != nil {
		return err
	}

	err = r.r.Scan(targets...)
	if err != nil {
		return err
	}

	r.scanDestinations = make([]reflect.Value, len(r.columns))
	return nil
}

func (r *Row) createTargets() ([]any, error) {
	targets := make([]any, len(r.columns))

	for i, name := range r.columns {
		dest := r.scanDestinations[i]
		if dest != zeroValue {
			targets[i] = dest.Interface()
			continue
		}

		if !r.allowUnknown {
			err := fmt.Errorf("no destination for column %s", name)
			return nil, createError(err, "no destination", name)
		}

		// See https://github.com/golang/go/issues/41607
		// Some drivers cannot work with nil values, so valid pointers should be
		// used for all column targets, even if they are discarded afterwards.
		targets[i] = new(any)
	}

	return targets, nil
}
