package scan

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
)

type TypeConverter interface {
	// ConvertType modifies the type of the struct
	// the method is called with the expected type of the column
	ConvertType(reflect.Type) reflect.Type

	// OriginalValue retrieves the original value from the converted type
	// the value given is a value of the type returned by ConvertType
	OriginalValue(reflect.Value) reflect.Value
}

// RowValidator is called with pointer to all the values from a row
// to determine if the row is valid
// if it is not, the zero type for that row is returned
type RowValidator = func(cols []string, vals []reflect.Value) bool

type contextKey string

// CtxKeyAllowUnknownColumns makes it possible to allow unknown columns using the context
var CtxKeyAllowUnknownColumns contextKey = "allow unknown columns"

type mappingOptions struct {
	typeConverter   TypeConverter
	rowValidator    RowValidator
	mapperMods      []MapperMod
	structTagPrefix string
}

// Uses reflection to create a mapping function for a struct type
// using the default options
func StructMapper[T any](opts ...MappingOption) Mapper[T] {
	return CustomStructMapper[T](defaultStructMapper, opts...)
}

// Uses reflection to create a mapping function for a struct type
// using with custom options
func CustomStructMapper[T any](src StructMapperSource, optMod ...MappingOption) Mapper[T] {
	opts := mappingOptions{}
	for _, o := range optMod {
		o.apply(&opts)
	}

	mod := func(ctx context.Context, c cols) (func(*Row) (any, error), func(any) (T, error)) {
		return structMapperFrom[T](ctx, c, src, opts)
	}

	if len(opts.mapperMods) > 0 {
		mod = Mod(mod, opts.mapperMods...)
	}

	return mod
}

func structMapperFrom[T any](ctx context.Context, c cols, s StructMapperSource, opts mappingOptions) (func(*Row) (any, error), func(any) (T, error)) {
	typ := typeOf[T]()

	isPointer, err := checks(typ)
	if err != nil {
		return ErrorMapper[T](err)
	}

	mapping, err := s.getMapping(typ)
	if err != nil {
		return ErrorMapper[T](err)
	}

	return mapperFromMapping[T](mapping, typ, isPointer, opts)(ctx, c)
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

type StructMapperSource interface {
	getMapping(reflect.Type) (mapping, error)
}

// MappingeOption is a function type that changes how the mapper is generated
type MappingOption interface {
	apply(*mappingOptions)
}

type mappingOptionFunc[T any] func(*mappingOptions)

func (m mappingOptionFunc[T]) apply(o *mappingOptions) {
	m(o)
}

// WithRowValidator sets the [RowValidator] for the struct mapper
// after scanning all values in a row, they are passed to the RowValidator
// if it returns false, the zero value for that row is returned
func WithRowValidator(rv RowValidator) MappingOption {
	return mappingOptionFunc[any](func(opt *mappingOptions) {
		opt.rowValidator = rv
	})
}

// TypeConverter sets the [TypeConverter] for the struct mapper
// it is called to modify the type of a column and get the original value back
func WithTypeConverter(tc TypeConverter) MappingOption {
	return mappingOptionFunc[any](func(opt *mappingOptions) {
		opt.typeConverter = tc
	})
}

// WithStructTagPrefix should be used when every column from the database has a prefix.
func WithStructTagPrefix(prefix string) MappingOption {
	return mappingOptionFunc[any](func(opt *mappingOptions) {
		opt.structTagPrefix = prefix
	})
}

// WithMapperMods accepts mods used to modify the mapper
func WithMapperMods(mods ...MapperMod) MappingOption {
	return mappingOptionFunc[any](func(opt *mappingOptions) {
		opt.mapperMods = append(opt.mapperMods, mods...)
	})
}

// NewStructMapperSource creates a new Mapping object with provided list of options.
func NewStructMapperSource(opts ...MappingSourceOption) (StructMapperSource, error) {
	src := newDefaultMapperSourceImpl()
	for _, o := range opts {
		if err := o(src); err != nil {
			return nil, err
		}
	}
	return src, nil
}

// MappingSourceOption are options to modify how a struct's mappings are interpreted
type MappingSourceOption func(src *mapperSourceImpl) error

// WithStructTagKey allows to use a custom struct tag key.
// The default tag key is `db`.
func WithStructTagKey(tagKey string) MappingSourceOption {
	return func(src *mapperSourceImpl) error {
		src.structTagKey = tagKey
		return nil
	}
}

// WithColumnSeparator allows to use a custom separator character for column name when combining nested structs.
// The default separator is "." character.
func WithColumnSeparator(separator string) MappingSourceOption {
	return func(src *mapperSourceImpl) error {
		src.columnSeparator = separator
		return nil
	}
}

// WithFieldNameMapper allows to use a custom function to map field name to column names.
// The default function maps fields names to "snake_case"
func WithFieldNameMapper(mapperFn NameMapperFunc) MappingSourceOption {
	return func(src *mapperSourceImpl) error {
		src.fieldMapperFn = mapperFn
		return nil
	}
}

// WithScannableTypes specifies a list of interfaces that underlying database library can scan into.
// In case the destination type passed to scan implements one of those interfaces,
// scan will handle it as primitive type case i.e. simply pass the destination to the database library.
// Instead of attempting to map database columns to destination struct fields or map keys.
// In order for reflection to capture the interface type, you must pass it by pointer.
//
// For example your database library defines a scanner interface like this:
//
//	type Scanner interface {
//	    Scan(...) error
//	}
//
// You can pass it to scan this way:
// scan.WithScannableTypes((*Scanner)(nil)).
func WithScannableTypes(scannableTypes ...any) MappingSourceOption {
	return func(src *mapperSourceImpl) error {
		for _, stOpt := range scannableTypes {
			st := reflect.TypeOf(stOpt)
			if st == nil {
				return fmt.Errorf("scannable type must be a pointer, got %T", stOpt)
			}
			if st.Kind() != reflect.Pointer {
				return fmt.Errorf("scannable type must be a pointer, got %s: %s",
					st.Kind(), st.String())
			}
			st = st.Elem()
			if st.Kind() != reflect.Interface {
				return fmt.Errorf("scannable type must be a pointer to an interface, got %s: %s",
					st.Kind(), st.String())
			}
			src.scannableTypes = append(src.scannableTypes, st)
		}
		return nil
	}
}

// mapperSourceImpl is an implementation of StructMapperSource.
type mapperSourceImpl struct {
	structTagKey    string
	columnSeparator string
	fieldMapperFn   NameMapperFunc
	scannableTypes  []reflect.Type
	maxDepth        int
	cache           map[reflect.Type]mapping
	mutex           sync.RWMutex
}

func (s *mapperSourceImpl) getMapping(typ reflect.Type) (mapping, error) {
	s.mutex.RLock()
	m, ok := s.cache[typ]
	s.mutex.RUnlock()

	if ok {
		return m, nil
	}

	s.setMappings(typ, "", make(visited), &m, nil)

	s.mutex.Lock()
	s.cache[typ] = m
	s.mutex.Unlock()

	return m, nil
}

func (s *mapperSourceImpl) setMappings(typ reflect.Type, prefix string, v visited, m *mapping, inits [][]int, position ...int) {
	count := v[typ]
	if count > s.maxDepth {
		return
	}
	v[typ] = count + 1

	var hasExported bool

	var isPointer bool
	if typ.Kind() == reflect.Pointer {
		isPointer = true
		typ = typ.Elem()
	}

	// If it implements a scannable type, then it can be used
	// as a value itself. Return it
	for _, scannable := range s.scannableTypes {
		if reflect.PtrTo(typ).Implements(scannable) {
			*m = append(*m, mapinfo{
				name:      prefix,
				position:  position,
				init:      inits,
				isPointer: isPointer,
			})
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
		tag := strings.Split(field.Tag.Get(s.structTagKey), ",")[0]
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
		var isPointer bool

		if fieldType.Kind() == reflect.Pointer {
			inits = append(inits, currentIndex)
			fieldType = fieldType.Elem()
			isPointer = true
		}

		if fieldType.Kind() == reflect.Struct {
			s.setMappings(field.Type, key, v.copy(), m, inits, currentIndex...)
			continue
		}

		*m = append(*m, mapinfo{
			name:      key,
			position:  currentIndex,
			init:      inits,
			isPointer: isPointer,
		})
	}

	// If it has no exported field (such as time.Time) then we attempt to
	// directly scan into it
	if !hasExported {
		*m = append(*m, mapinfo{
			name:      prefix,
			position:  position,
			init:      inits,
			isPointer: isPointer,
		})
	}
}

func filterColumns(ctx context.Context, c cols, m mapping, prefix string) (mapping, error) {
	// Filter the mapping so we only ask for the available columns
	filtered := make(mapping, 0, len(c))
	for _, name := range c {
		key := name
		if prefix != "" {
			if !strings.HasPrefix(name, prefix) {
				continue
			}

			key = name[len(prefix):]
		}

		for _, info := range m {
			if key == info.name {
				info.name = name
				filtered = append(filtered, info)
				break
			}
		}
	}

	return filtered, nil
}

func mapperFromMapping[T any](m mapping, typ reflect.Type, isPointer bool, opts mappingOptions) func(context.Context, cols) (func(*Row) (any, error), func(any) (T, error)) {
	return func(ctx context.Context, c cols) (func(*Row) (any, error), func(any) (T, error)) {
		// Filter the mapping so we only ask for the available columns
		filtered, err := filterColumns(ctx, c, m, opts.structTagPrefix)
		if err != nil {
			return ErrorMapper[T](err)
		}

		mapper := regular[T]{
			typ:       typ,
			isPointer: isPointer,
			filtered:  filtered,
			converter: opts.typeConverter,
			validator: opts.rowValidator,
		}
		switch {
		case opts.typeConverter == nil && opts.rowValidator == nil:
			return mapper.regular()

		default:
			return mapper.allOptions()
		}
	}
}

type regular[T any] struct {
	isPointer bool
	typ       reflect.Type
	filtered  mapping
	converter TypeConverter
	validator RowValidator
}

func (s regular[T]) regular() (func(*Row) (any, error), func(any) (T, error)) {
	return func(v *Row) (any, error) {
			var row reflect.Value
			if s.isPointer {
				row = reflect.New(s.typ.Elem()).Elem()
			} else {
				row = reflect.New(s.typ).Elem()
			}

			for _, info := range s.filtered {
				for _, v := range info.init {
					pv := row.FieldByIndex(v)
					if !pv.IsZero() {
						continue
					}

					pv.Set(reflect.New(pv.Type().Elem()))
				}

				fv := row.FieldByIndex(info.position)
				v.ScheduleScanx(info.name, fv.Addr())
			}

			return row, nil
		}, func(v any) (T, error) {
			row := v.(reflect.Value)

			if s.isPointer {
				row = row.Addr()
			}

			return row.Interface().(T), nil
		}
}

func (s regular[T]) allOptions() (func(*Row) (any, error), func(any) (T, error)) {
	return func(v *Row) (any, error) {
			row := make([]reflect.Value, len(s.filtered))

			for i, info := range s.filtered {
				var ft reflect.Type
				if s.isPointer {
					ft = s.typ.Elem().FieldByIndex(info.position).Type
				} else {
					ft = s.typ.FieldByIndex(info.position).Type
				}

				if s.converter != nil {
					ft = s.converter.ConvertType(ft)
				}

				row[i] = reflect.New(ft)
				v.ScheduleScanx(info.name, row[i])
			}

			return row, nil
		}, func(v any) (T, error) {
			vals := v.([]reflect.Value)

			if s.validator != nil && !s.validator(s.filtered.cols(), vals) {
				var t T
				return t, nil
			}

			var row reflect.Value
			if s.isPointer {
				row = reflect.New(s.typ.Elem()).Elem()
			} else {
				row = reflect.New(s.typ).Elem()
			}

			for i, info := range s.filtered {
				for _, v := range info.init {
					pv := row.FieldByIndex(v)
					if !pv.IsZero() {
						continue
					}

					pv.Set(reflect.New(pv.Type().Elem()))
				}

				val := vals[i].Elem()
				if s.converter != nil {
					val = s.converter.OriginalValue(val)
				}

				fv := row.FieldByIndex(info.position)
				if info.isPointer {
					fv.Elem().Set(val)
				} else {
					fv.Set(val)
				}
			}

			if s.isPointer {
				row = row.Addr()
			}

			return row.Interface().(T), nil
		}
}

//nolint:gochecknoglobals
func newDefaultMapperSourceImpl() *mapperSourceImpl {
	return &mapperSourceImpl{
		structTagKey:    "db",
		columnSeparator: ".",
		fieldMapperFn:   snakeCaseFieldFunc,
		scannableTypes:  []reflect.Type{reflect.TypeOf((*sql.Scanner)(nil)).Elem()},
		maxDepth:        3,
		cache:           make(map[reflect.Type]mapping),
	}
}

var defaultStructMapper = newDefaultMapperSourceImpl()
