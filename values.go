package scan

import (
	"errors"
	"fmt"
	"reflect"
)

var zeroValue reflect.Value

// Value retrieves a value from [Values] with the specified name
// if [Values.IsRecording] returns true, it will ALWAYS return the zero value
// of that type
// When not recording, it will panic if the requested type does not match
// what was recorded
func Value[T any](v *Values, name string) T {
	if v.recording {
		v.record(name, typeOf[T]())
	}

	i := v.get(v.index(name))
	if i == nil {
		var zero T
		return zero
	}

	return i.(T)
}

// ReflectedValue returns the named value as an [reflect.Value]
// When not recording, it will panic if the requested type does not match
// what was recorded
func ReflectedValue(v *Values, name string, typ reflect.Type) reflect.Value {
	if v.recording {
		v.record(name, typ)
	}

	return v.getRef(v.index(name))
}

func newValues(r Rows, allowUnknown bool) (*Values, error) {
	cols, err := r.Columns()
	if err != nil {
		return nil, err
	}

	return &Values{
		columns:         cols,
		scanDestination: make([]reflect.Value, len(cols)),
		types:           make([]reflect.Type, len(cols)),
	}, nil
}

// Values holds the values of a row
// use Value() to retrieve a value by column name
// Column names must be unique, so
// if multiple columns have the same name, only the last one remains
type Values struct {
	columns         []string
	recording       bool
	types           []reflect.Type
	scanned         []any
	allowUnknown    bool
	scanDestination []reflect.Value
	store           []any
}

// IsRecording returns wether the values are currently in recording mode
// When recording, calls to Get() will record the expected type
func (v *Values) IsRecording() bool {
	return v.recording
}

func (v *Values) startRecording() {
	v.recording = true
}

func (v *Values) index(name string) int {
	for i, n := range v.columns {
		if name == n {
			return i
		}
	}

	return -1
}

func (v *Values) findMissingColumns() []string {
	cols := make([]string, len(v.columns))

	for index, name := range v.columns {
		if v.scanDestination[index] != zeroValue {
			continue
		}

		if t := v.types[index]; t != nil {
			continue
		}

		cols[index] = name // to preserve order
	}

	// Remvoe empty strings
	filtered := make([]string, 0)
	for _, v := range cols {
		if v != "" {
			filtered = append(filtered, v)
		}
	}

	return filtered
}

func (v *Values) stopRecording() error {
	if !v.allowUnknown {
		missing := v.findMissingColumns()
		if len(missing) > 0 {
			err := fmt.Errorf("No destination for columns %v", missing)
			return createError(err, append([]string{"no destination"}, missing...)...)
		}
	}

	v.recording = false
	return nil
}

// To get a copy of the columns to pass to mapper generators
// since modifing the map can have unintended side effects.
// Ideally, a generator should only call this once
func (v *Values) columnsCopy() []string {
	m := make([]string, len(v.columns))
	copy(m, v.columns)
	return m
}

func (v *Values) getRef(index int) reflect.Value {
	if index == -1 {
		return zeroValue
	}

	if v.recording {
		t := v.types[index]
		return reflect.New(t).Elem()
	}

	return reflect.Indirect(
		reflect.ValueOf(v.scanned[index]),
	)
}

func (v *Values) get(index int) any {
	ref := v.getRef(index)

	if ref == zeroValue {
		if v.recording {
			return nil
		}

		return v.scanned[index]
	}

	return ref.Interface()
}

func (v *Values) record(name string, t reflect.Type) {
	i := v.index(name)
	if i == -1 {
		return
	}
	v.types[i] = t
}

func (v *Values) scanRow(r Row) error {
	targets := make([]any, len(v.columns))

	for i := range v.columns {
		if dest := v.scanDestination[i]; dest != zeroValue {
			targets[i] = dest.Addr().Interface()
			continue
		}

		t := v.types[i]
		if t == nil {
			var fallback any
			targets[i] = &fallback

			continue
		}

		if t.Kind() == reflect.Pointer {
			targets[i] = reflect.New(t).Elem().Interface()
		} else {
			targets[i] = reflect.New(t).Interface()
		}
	}

	err := r.Scan(targets...)
	if err != nil {
		return err
	}

	v.scanned = targets
	v.scanDestination = make([]reflect.Value, len(v.columns))
	return nil
}

func (v *Values) setScanDestination(colName string, val reflect.Value) error {
	if !val.CanAddr() {
		return createError(errors.New("inaddressable value given as scan destination"), colName)
	}

	colIndex := v.index(colName)
	if colIndex == -1 {
		return createError(errors.New("unknown column to map to"), colName)
	}

	v.scanDestination[colIndex] = val

	return nil
}

func (v *Values) saveInStore(val any) (key int) {
	v.store = append(v.store, val)
	return len(v.store) - 1
}

func (v *Values) getFromStore(key int) any {
	return v.store[key]
}
