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

var (
	matchFirstCapRe     = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCapRe       = regexp.MustCompile("([a-z0-9])([A-Z])")
	defaultStructMapper = newDefaultMapperSourceImpl()
)

// snakeCaseFieldFunc is a NameMapperFunc that maps struct field to snake case.
func snakeCaseFieldFunc(str string) string {
	snake := matchFirstCapRe.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCapRe.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

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
func WithFieldNameMapper(mapperFn func(string) string) MappingSourceOption {
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
	fieldMapperFn   func(string) string
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

			name := tag
			if tag == "" {
				name = s.fieldMapperFn(field.Name)
			}

			key = strings.Join([]string{key, name}, sep)
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
