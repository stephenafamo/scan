package scan

import (
	"reflect"
)

// Value retrieves a value from [Values] with the specified name
// if [Values.IsRecording()] is true, it will ALWAYS return the zero value
// of that type
// When not recording, it will panic if the requested type does not match
// what was recorded
func Value[T any](v *Values, name string) T {
	if v.recording {
		var x T
		v.record(name, reflect.TypeOf(x))
	}

	return v.get(name).(T)
}

// ReflectedValue returns the named value as an [reflect.Value]
// When not recording, it will panic if the requested type does not match
// what was recorded
func ReflectedValue(v *Values, name string, typ reflect.Type) reflect.Value {
	if v.recording {
		v.record(name, typ)
	}

	return v.getRef(name)
}

func newValues(r Rows) (*Values, error) {
	cols, err := r.Columns()
	if err != nil {
		return nil, err
	}

	// convert columns to a map
	colMap := make(map[string]int, len(cols))
	for k, v := range cols {
		colMap[v] = k
	}

	return &Values{
		columns: colMap,
		types:   make(map[string]reflect.Type, len(cols)),
	}, nil
}

// Values holds the values of a row
// use Value() to retrieve a value by column name
// Column names must be unique, so
// if multiple columns have the same name, only the last one remains
type Values struct {
	columns   map[string]int
	recording bool
	types     map[string]reflect.Type
	scanned   []any
}

// IsRecording returns wether the values are currently in recording mode
// When recording, calls to Get() will record the expected type
func (v *Values) IsRecording() bool {
	return v.recording
}

// To get a copy of the columns to pass to mapper generators
// since modifing the map can have unintended side effects.
// Ideally, a generator should only call this once
func (v *Values) columnsCopy() map[string]int {
	m := make(map[string]int, len(v.columns))
	for k, v := range v.columns {
		m[k] = v
	}
	return m
}

func (v *Values) getRef(name string) reflect.Value {
	index, ok := v.columns[name]
	if !ok || v.recording {
		x := reflect.New(v.types[name]).Elem()
		return x
	}

	return reflect.Indirect(
		reflect.ValueOf(v.scanned[index]),
	)
}

func (v *Values) get(name string) any {
	return v.getRef(name).Interface()
}

func (v *Values) record(name string, t reflect.Type) {
	v.types[name] = t
}

func (v *Values) scanRow(r Row) error {
	pointers := make([]any, len(v.columns))

	for name, i := range v.columns {
		t := v.types[name]
		if t == nil {
			var fallback interface{}
			pointers[i] = &fallback

			continue
		}

		pointers[i] = reflect.New(t).Interface()
	}

	err := r.Scan(pointers...)
	if err != nil {
		return err
	}

	v.scanned = pointers
	return nil
}
