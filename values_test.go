package scan

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestValueRecording(t *testing.T) {
	Columns := map[string]any{
		"bool":       false,
		"*bool":      ptr(false),
		"string":     "",
		"*string":    ptr(""),
		"[]byte":     []byte(""),
		"*[]byte":    ptr([]byte("")),
		"int":        0,
		"*int":       ptr(0),
		"int64":      int64(0),
		"*int64":     ptr(int64(0)),
		"float32":    float32(0),
		"*float32":   ptr(float32(0)),
		"float64":    float64(0),
		"*float64":   ptr(float64(0)),
		"time.Time":  time.Time{},
		"*time.Time": ptr(time.Time{}),
	}

	vals := &Values{
		types:     make([]reflect.Type, len(Columns)),
		recording: true,
	}

	for k, v := range Columns {
		vals.columns = append(vals.columns, k)
		vals.scanned = append(vals.scanned, reflect.New(reflect.TypeOf(v)).Interface())
	}

	// record the values
	for k, v := range Columns {
		ReflectedValue(vals, k, reflect.TypeOf(v))
	}

	// turn off recording
	vals.recording = false

	// get the values and check the type
	for k, v := range Columns {
		t.Run(k, func(t *testing.T) {
			vTyp := reflect.TypeOf(v)
			gotten := ReflectedValue(vals, k, vTyp).Interface()
			// We expect the zero value because there was no
			// scanning
			expected := reflect.Zero(vTyp).Interface()

			if diff := cmp.Diff(expected, gotten); diff != "" {
				t.Fatalf("diff: %s", diff)
			}
		})
	}
}
