package scan

import (
	"fmt"
	"reflect"
)

var zeroValue reflect.Value

func newValues(r Rows, allowUnknown bool) (*Values, error) {
	cols, err := r.Columns()
	if err != nil {
		return nil, err
	}

	return &Values{
		columns:          cols,
		scanDestinations: make([]reflect.Value, len(cols)),
	}, nil
}

// Values holds the values of a row
// use Value() to retrieve a value by column name
// Column names must be unique, so
// if multiple columns have the same name, only the last one remains
type Values struct {
	columns             []string
	scanned             []any
	allowUnknown        bool
	scanDestinations    []reflect.Value
	unknownDestinations []string
}

func (v *Values) index(name string) int {
	for i, n := range v.columns {
		if name == n {
			return i
		}
	}

	return -1
}

// To get a copy of the columns to pass to mapper generators
// since modifing the map can have unintended side effects.
// Ideally, a generator should only call this once
func (v *Values) columnsCopy() []string {
	m := make([]string, len(v.columns))
	copy(m, v.columns)
	return m
}

func (v *Values) scanRow(r Row) error {
	if len(v.unknownDestinations) > 0 {
		return createError(fmt.Errorf("unknown columns to map to: %v", v.unknownDestinations), v.unknownDestinations...)
	}

	targets := make([]any, len(v.columns))

	for i, name := range v.columns {
		dest := v.scanDestinations[i]
		if dest != zeroValue {
			targets[i] = dest.Interface()
			continue
		}

		if !v.allowUnknown {
			err := fmt.Errorf("No destination for column %s", name)
			return createError(err, "no destination", name)
		}
	}

	err := r.Scan(targets...)
	if err != nil {
		return err
	}

	v.scanned = targets
	v.scanDestinations = make([]reflect.Value, len(v.columns))
	return nil
}

func (v *Values) ScheduleScan(colName string, val any) {
	v.ScheduleScanx(colName, reflect.ValueOf(val))
}

func (v *Values) ScheduleScanx(colName string, val reflect.Value) {
	colIndex := v.index(colName)
	if colIndex == -1 {
		v.unknownDestinations = append(v.unknownDestinations, colName)
		return
	}

	v.scanDestinations[colIndex] = val
}
