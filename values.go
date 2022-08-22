package scan

import (
	"fmt"
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

// ValueCallback passes a function that will be called for the column
// this is meant to be used when you need to control the exact value that the column
// is mapped into.
func ValueCallback(v *Values, name string, f func() reflect.Value) reflect.Value {
	if v.recording {
		v.recordCallback(name, f)
	}

	return v.getRef(name)
}

func newValues(r Rows, allowUnknown bool) (*Values, error) {
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
		columns:       colMap,
		types:         make(map[string]reflect.Type, len(cols)),
		pointerGetter: make(map[string]func() reflect.Value),
	}, nil
}

// Values holds the values of a row
// use Value() to retrieve a value by column name
// Column names must be unique, so
// if multiple columns have the same name, only the last one remains
type Values struct {
	columns       map[string]int
	recording     bool
	types         map[string]reflect.Type
	pointerGetter map[string]func() reflect.Value
	scanned       []any
	allowUnknown  bool
}

// IsRecording returns wether the values are currently in recording mode
// When recording, calls to Get() will record the expected type
func (v *Values) IsRecording() bool {
	return v.recording
}

func (v *Values) startRecording() {
	v.recording = true
}

func (v *Values) findMiddingColumns() []string {
	var cols []string
	for name := range v.columns {
		if _, ok := v.types[name]; !ok {
			cols = append(cols, name)
		}
	}

	return cols
}

func (v *Values) stopRecording() error {
	if !v.allowUnknown && len(v.types) != len(v.columns) {
		missing := v.findMiddingColumns()
		err := fmt.Errorf("No destination for columns %v", missing)
		return createError(err, append([]string{"no destination"}, missing...)...)
	}

	v.recording = false
	return nil
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
		if p, ok := v.pointerGetter[name]; ok {
			return p()
		}

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

func (v *Values) recordCallback(name string, f func() reflect.Value) {
	v.types[name] = f().Type()
	v.pointerGetter[name] = f
}

func (v *Values) scanRow(r Row) error {
	targets := make([]any, len(v.columns))

	for name, i := range v.columns {
		if p, ok := v.pointerGetter[name]; ok {
			targets[i] = p().Interface()
			continue
		}

		t := v.types[name]
		if t == nil {
			var fallback interface{}
			targets[i] = &fallback

			continue
		}

		// pointers[i] = reflect.New(t).Interface()
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
	return nil
}
