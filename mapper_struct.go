package scan

import (
	"context"
	"fmt"
	"reflect"
)

// CtxKeyAllowUnknownColumns makes it possible to allow unknown columns using the context
var CtxKeyAllowUnknownColumns contextKey = "allow unknown columns"

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
		o(&opts)
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

type mappingOptions struct {
	typeConverter   TypeConverter
	rowValidator    RowValidator
	mapperMods      []MapperMod
	structTagPrefix string
}

// MappingeOption is a function type that changes how the mapper is generated
type MappingOption func(*mappingOptions)

// WithRowValidator sets the [RowValidator] for the struct mapper
// after scanning all values in a row, they are passed to the RowValidator
// if it returns false, the zero value for that row is returned
func WithRowValidator(rv RowValidator) MappingOption {
	return func(opt *mappingOptions) {
		opt.rowValidator = rv
	}
}

// TypeConverter sets the [TypeConverter] for the struct mapper
// it is called to modify the type of a column and get the original value back
func WithTypeConverter(tc TypeConverter) MappingOption {
	return func(opt *mappingOptions) {
		opt.typeConverter = tc
	}
}

// WithStructTagPrefix should be used when every column from the database has a prefix.
func WithStructTagPrefix(prefix string) MappingOption {
	return func(opt *mappingOptions) {
		opt.structTagPrefix = prefix
	}
}

// WithMapperMods accepts mods used to modify the mapper
func WithMapperMods(mods ...MapperMod) MappingOption {
	return func(opt *mappingOptions) {
		opt.mapperMods = append(opt.mapperMods, mods...)
	}
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
					row[i] = s.converter.TypeToDestination(ft)
				} else {
					row[i] = reflect.New(ft)
				}

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

				var val reflect.Value
				if s.converter != nil {
					val = s.converter.ValueFromDestination(vals[i])
				} else {
					val = vals[i].Elem()
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
