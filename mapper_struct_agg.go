package scan

import (
	"context"
	"fmt"
	"reflect"
)

type AggConverter interface {
	// ConvertToAgg should convert a type of the struct's field to a type
	// that the executor can scan into
	// For example, for postgres, ConvertToAgg may convert [int] to pq.GenericArray[int]
	ConvertToAgg(reflect.Type) reflect.Value

	// ConvertFromAgg should convert an aggregate scannable type to a slice of the field
	// For example, for postgres, ConvertFromAgg may convert [pq.GenericArray[int]] to [[]int]
	ConvertFromAgg(reflect.Value) reflect.Value
}

// AggStructMapper is like [StructMapper] but is useful when a single row contains
// aggregate values for multiple rows
func AggStructMapper[T any, Ts ~[]T](converter AggConverter) Mapper[Ts] {
	return func(ctx context.Context, c cols) func(*Values) (Ts, error) {
		return aggStructMapperFrom[T, Ts](ctx, c, defaultStructMapper, converter)
	}
}

// AggCustomStructMapper is like [CustomStructMapper] but is useful when a single row contains
// aggregate values for multiple rows
func AggCustomStructMapper[T any, Ts ~[]T](converter AggConverter, opts ...MappingOption) Mapper[Ts] {
	return func(ctx context.Context, c cols) func(*Values) (Ts, error) {
		mapper, err := newStructMapper(opts...)
		if err != nil {
			return errorMapper[Ts](err)
		}
		return aggStructMapperFrom[T, Ts](ctx, c, *mapper, converter)
	}
}

func aggStructMapperFrom[T any, Ts ~[]T](ctx context.Context, c cols, s structMapper, converter AggConverter) func(*Values) (Ts, error) {
	var x T
	typ := reflect.TypeOf(x)

	isPointer, err := checks(typ)
	if err != nil {
		return errorMapper[Ts](err)
	}

	mapping, err := s.getMapping(typ)
	if err != nil {
		return errorMapper[Ts](err)
	}

	return aggMapperFromMapping[T, Ts](converter, mapping, typ, isPointer, s.allowUnknownColumns)(ctx, c)
}

func aggMapperFromMapping[T any, Ts ~[]T](converter AggConverter, m mapping, typ reflect.Type, isPointer, allowUnknown bool) func(context.Context, cols) func(*Values) (Ts, error) {
	if isPointer {
		typ = typ.Elem()
	}

	return func(ctx context.Context, c cols) func(*Values) (Ts, error) {
		// Filter the mapping so we only ask for the available columns
		filtered, err := filterColumns(ctx, c, m, allowUnknown)
		if err != nil {
			return errorMapper[Ts](err)
		}

		return func(v *Values) (Ts, error) {
			var all Ts

			if len(filtered) == 0 {
				return nil, nil
			}

			length := -1

			for name, info := range filtered {
				ft := typ.FieldByIndex(info.position)
				aggType := converter.ConvertToAgg(ft.Type)
				aggValue := ValueCallback(v, name, func() reflect.Value {
					return aggType
				})

				value := converter.ConvertFromAgg(aggValue)
				if value.Kind() != reflect.Slice {
					return nil, fmt.Errorf("Converted value of type %q is not a slice", value.Type().String())
				}

				if value.Type().Elem() != ft.Type {
					return nil, fmt.Errorf(
						"Converted value is a slice of %s but expects a slice of %q",
						value.Type().Elem().String(), ft.Type.String())
				}

				if length == -1 { // not set
					length = value.Len()
					all = make(Ts, length)
				}

				if length != value.Len() {
					return nil, fmt.Errorf("Column %q has length %d but expected %d", name, value.Len(), length)
				}

				allVal := reflect.ValueOf(all)
				for i := 0; i < length; i++ {
					one := allVal.Index(i)
					if isPointer {
						if one.IsZero() {
							one.Set(reflect.New(typ))
						}

						one = one.Elem()
					}

					for _, v := range info.init {
						if pv := one.FieldByIndex(v); pv.IsZero() {
							pv.Set(reflect.New(pv.Type().Elem()))
						}
					}

					fv := one.FieldByIndex(info.position)
					fv.Set(value.Index(i))

					// one := all.Index(i)
					if isPointer {
						allVal.Index(i).Set(one.Addr())
					} else {
						allVal.Index(i).Set(one)
					}
				}
			}

			return all, nil
		}
	}
}
