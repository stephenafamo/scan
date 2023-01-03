package scan

import (
	"context"
)

type (
	// MapperMod is a function that can be used to convert an existing mapper
	// into a new mapper using [Mod]
	MapperMod = func(context.Context, cols) (BeforeFunc, AfterMod)
	// AfterMod receives both the link of the [MapperMod] and the retrieved value from
	// the original mapper
	AfterMod = func(link any, retrieved any) error
)

// Mod converts an existing mapper into a new mapper with [MapperMod]s
func Mod[T any](m Mapper[T], mods ...MapperMod) Mapper[T] {
	return func(ctx context.Context, c cols) (func(*Row) (any, error), func(any) (T, error)) {
		before, after := m(ctx, c)
		befores := make([]BeforeFunc, len(mods))
		afters := make([]AfterMod, len(mods))
		links := make([]any, len(mods))
		for i, m := range mods {
			befores[i], afters[i] = m(ctx, c)
		}

		return func(v *Row) (any, error) {
				a, err := before(v)
				if err != nil {
					return nil, err
				}

				for i, b := range befores {
					if links[i], err = b(v); err != nil {
						return nil, err
					}
				}

				return a, nil
			}, func(v any) (T, error) {
				t, err := after(v)
				if err != nil {
					return t, err
				}

				for i, a := range afters {
					if err := a(links[i], t); err != nil {
						return t, err
					}
				}

				return t, nil
			}
	}
}
