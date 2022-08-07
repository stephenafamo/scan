package scan

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

var (
	colsTyp = reflect.TypeOf(cols{})
	valsTyp = reflect.TypeOf(&Values{})
	errTyp  = reflect.TypeOf((*error)(nil)).Elem()
	ctxTyp  = reflect.TypeOf((*context.Context)(nil)).Elem()
)

// Uses reflection to create a mapping function for a struct type
// using the default options
func StructMapper[T any](ctx context.Context, c cols) func(*Values) (T, error) {
	return structMapperFrom[T](ctx, c, defaultStructMapper)
}

// Uses reflection to create a mapping function for a struct type
// using with custom options
func CustomStructMapper[T any](opts ...MappingOption) func(context.Context, cols) func(*Values) (T, error) {
	return func(ctx context.Context, c cols) func(*Values) (T, error) {
		mapper, err := newStructMapper(opts...)
		if err != nil {
			return errorMapper[T](err)
		}
		return structMapperFrom[T](ctx, c, *mapper)
	}
}

func structMapperFrom[T any](ctx context.Context, c cols, s structMapper) func(*Values) (T, error) {
	var x T
	typ := reflect.TypeOf(x)

	isPointer, err := checks(typ)
	if err != nil {
		return errorMapper[T](err)
	}

	if m, ok := mappable[T](typ, isPointer); ok {
		return m(ctx, c)
	}

	mapping, err := s.getMapping(typ, isPointer)
	if err != nil {
		return errorMapper[T](err)
	}

	return mapperFromMapping[T](mapping, typ, isPointer, s.allowUnknownColumns)(c)
}

// Check if there are any errors, and returns if it is a pointer or not
func checks(typ reflect.Type) (bool, error) {
	if typ == nil {
		return false, fmt.Errorf("Nil type passed to StructMapper")
	}

	var isPointer bool

	switch {
	case typ.Kind() == reflect.Struct:
	case typ.Kind() == reflect.Pointer:
		isPointer = true

		if typ.Elem().Kind() != reflect.Struct {
			return false, fmt.Errorf("Type %q is not a struct or pointer to a struct", typ.String())
		}
	default:
		return false, fmt.Errorf("Type %q is not a struct or pointer to a struct", typ.String())
	}

	return isPointer, nil
}

func mappable[T any](typ reflect.Type, isPointer bool) (func(context.Context, cols) func(*Values) (T, error), bool) {
	var t, pt reflect.Type
	if isPointer {
		t = typ.Elem()
		pt = typ
	} else {
		t = typ
		pt = reflect.PointerTo(typ)
	}

	// Check if the type has a methodTyp called "MapValues"
	methodTyp, ok := pt.MethodByName("MapValues")
	if !ok {
		return nil, false
	}

	// If the response type is a pointer
	var pointerResp bool

	// func(*Values) (T, error)
	VFunc := reflect.FuncOf([]reflect.Type{valsTyp}, []reflect.Type{t, errTyp}, false)
	// func(*Values) (*T, error)
	PVFunc := reflect.FuncOf([]reflect.Type{valsTyp}, []reflect.Type{pt, errTyp}, false)

	switch methodTyp.Type {
	// func (*Typ) MapValues(cols) func(*Values) (T, error)
	case reflect.FuncOf([]reflect.Type{pt, ctxTyp, colsTyp}, []reflect.Type{VFunc}, false):
		pointerResp = false

	// func (*Typ) MapValues(cols) func(*Values) (*T, error)
	case reflect.FuncOf([]reflect.Type{pt, ctxTyp, colsTyp}, []reflect.Type{PVFunc}, false):
		pointerResp = true

	default:
		// Does not implement the method
		return nil, false
	}

	method := reflect.New(t).MethodByName("MapValues")

	// same return type... easy
	if isPointer == pointerResp {
		return func(ctx context.Context, c cols) func(*Values) (T, error) {
			return method.Call(
				[]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(c)},
			)[0].Interface().(func(*Values) (T, error))
		}, true
	}

	return func(ctx context.Context, c cols) func(*Values) (T, error) {
		f := method.Call(
			[]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(c)},
		)[0]

		return func(v *Values) (T, error) {
			out := f.Call(
				[]reflect.Value{reflect.ValueOf(c)},
			)

			val := out[0]
			err := out[1].Interface().(error)

			if pointerResp {
				return val.Addr().Interface().(T), err
			}

			return val.Elem().Interface().(T), err
		}
	}, true
}

// NameMapperFunc is a function type that maps a struct field name to the database column name.
type NameMapperFunc func(string) string

var (
	matchFirstCapRe = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCapRe   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

// snakeCaseFieldFunc is a NameMapperFunc that maps struct field to snake case.
func snakeCaseFieldFunc(str string) string {
	snake := matchFirstCapRe.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCapRe.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

// newStructMapper creates a new Mapping object with provided list of options.
func newStructMapper(opts ...MappingOption) (*structMapper, error) {
	api := defaultStructMapper
	for _, o := range opts {
		if err := o(&api); err != nil {
			return nil, err
		}
	}
	return &api, nil
}

// MappingOption is a function type that changes Mapping configuration.
type MappingOption func(api *structMapper) error

// WithStructTagKey allows to use a custom struct tag key.
// The default tag key is `db`.
func WithStructTagKey(tagKey string) MappingOption {
	return func(api *structMapper) error {
		api.structTagKey = tagKey
		return nil
	}
}

// WithColumnSeparator allows to use a custom separator character for column name when combining nested structs.
// The default separator is "." character.
func WithColumnSeparator(separator string) MappingOption {
	return func(api *structMapper) error {
		api.columnSeparator = separator
		return nil
	}
}

// WithFieldNameMapper allows to use a custom function to map field name to column names.
// The default function maps fields names to "snake_case"
func WithFieldNameMapper(mapperFn NameMapperFunc) MappingOption {
	return func(api *structMapper) error {
		api.fieldMapperFn = mapperFn
		return nil
	}
}

// WithScannableTypes specifies a list of interfaces that underlying database library can scan into.
// In case the destination type passed to dbscan implements one of those interfaces,
// dbscan will handle it as primitive type case i.e. simply pass the destination to the database library.
// Instead of attempting to map database columns to destination struct fields or map keys.
// In order for reflection to capture the interface type, you must pass it by pointer.
//
// For example your database library defines a scanner interface like this:
//
//	type Scanner interface {
//	    Scan(...) error
//	}
//
// You can pass it to dbscan this way:
// dbscan.WithScannableTypes((*Scanner)(nil)).
func WithScannableTypes(scannableTypes ...interface{}) MappingOption {
	return func(api *structMapper) error {
		for _, stOpt := range scannableTypes {
			st := reflect.TypeOf(stOpt)
			if st == nil {
				return fmt.Errorf("scannable type must be a pointer, got %T", st)
			}
			if st.Kind() != reflect.Ptr {
				return fmt.Errorf("scannable type must be a pointer, got %s: %s",
					st.Kind(), st.String())
			}
			st = st.Elem()
			if st.Kind() != reflect.Interface {
				return fmt.Errorf("scannable type must be a pointer to an interface, got %s: %s",
					st.Kind(), st.String())
			}
			api.scannableTypes = append(api.scannableTypes, st)
		}
		return nil
	}
}

// WithAllowUnknownColumns allows the scanner to ignore db columns that doesn't exist at the destination.
// The default function is to throw an error when a db column ain't found at the destination.
func WithAllowUnknownColumns(allowUnknownColumns bool) MappingOption {
	return func(api *structMapper) error {
		api.allowUnknownColumns = allowUnknownColumns
		return nil
	}
}

// structMapper is the core type in dbscan. It implements all the logic and exposes functionality available in the package.
// With structMapper type users can create a custom structMapper instance and override default settings hence configure dbscan.
type structMapper struct {
	structTagKey        string
	columnSeparator     string
	fieldMapperFn       NameMapperFunc
	scannableTypes      []reflect.Type
	allowUnknownColumns bool
	maxDepth            int
}

func (s structMapper) getMapping(typ reflect.Type, isPointer bool) (mapping, error) {
	if typ == nil {
		return nil, fmt.Errorf("Nil type passed to StructMapper")
	}

	if isPointer {
		typ = typ.Elem()
	}

	m := make(mapping)
	s.setMappings(typ, "", make(visited), m, nil)

	return m, nil
}

func (s structMapper) setMappings(typ reflect.Type, prefix string, v visited, m mapping, inits [][]int, position ...int) {
	count := v[typ]
	if count > s.maxDepth {
		return
	}
	v[typ] = count + 1

	var hasExported bool

	// If it implements a scannable type, then it can be used
	// as a value itself. Return it
	for _, scannable := range s.scannableTypes {
		if reflect.PtrTo(typ).Implements(scannable) {
			m[prefix] = mapinfo{
				position: position,
				init:     inits,
			}
			return
		}
	}

	// Go through the struct fields and populate the map.
	// Recursively go into any child structs, adding a prefix where necessary
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		// Don't consider unexported fields
		if !field.IsExported() {
			continue
		}

		// Skip columns that have the tag "-"
		tag := field.Tag.Get(s.structTagKey)
		if tag == "-" {
			continue
		}

		hasExported = true

		key := prefix

		if !field.Anonymous {
			var sep string
			if prefix != "" {
				sep = s.columnSeparator
			}

			name := field.Name
			if tag != "" {
				name = tag
			}

			key = strings.Join([]string{key, s.fieldMapperFn(name)}, sep)
		}

		currentIndex := append(position, i)
		fieldType := field.Type
		if fieldType.Kind() == reflect.Pointer {
			inits = append(inits, currentIndex)
			fieldType = fieldType.Elem()
		}

		if fieldType.Kind() == reflect.Struct {
			s.setMappings(fieldType, key, v.copy(), m, inits, currentIndex...)
			continue
		}

		m[key] = mapinfo{
			position: currentIndex,
			init:     inits,
		}
	}

	// If it has no exported field (such as time.Time) then we attempt to
	// directly scan into it
	if !hasExported {
		m[prefix] = mapinfo{
			position: position,
			init:     inits,
		}
	}
}

func mapperFromMapping[T any](m mapping, typ reflect.Type, isPointer, allowUnknown bool) func(cols) func(*Values) (T, error) {
	if isPointer {
		typ = typ.Elem()
	}

	return func(c cols) func(*Values) (T, error) {
		// Filter the mapping so we only ask for the available columns
		filtered := make(mapping)
		for name := range c {
			v, ok := m[name]
			if !ok {
				if !allowUnknown {
					err := fmt.Errorf("No destination for column %q", name)
					return errorMapper[T](err, "no destination", name)
				}
				continue
			}

			filtered[name] = v
		}

		return func(v *Values) (T, error) {
			row := reflect.New(typ).Elem()

			for name, info := range filtered {
				for _, v := range info.init {
					pv := row.FieldByIndex(v)
					if !pv.IsZero() {
						continue
					}

					pv.Set(reflect.New(pv.Type().Elem()))
				}

				fv := row.FieldByIndex(info.position)
				val := AnyValue(v, name, fv.Type())
				fv.Set(reflect.ValueOf(val))
			}

			if isPointer {
				row = row.Addr()
			}

			return row.Interface().(T), nil
		}
	}
}

//nolint:gochecknoglobals
var defaultStructMapper = structMapper{
	structTagKey:        "db",
	columnSeparator:     ".",
	fieldMapperFn:       snakeCaseFieldFunc,
	scannableTypes:      []reflect.Type{reflect.TypeOf((*sql.Scanner)(nil)).Elem()},
	allowUnknownColumns: false,
	maxDepth:            3,
}
