package scan

import (
	"context"
)

type (
	MapperMod func(context.Context, cols) (BeforeMod, AfterMod)
	BeforeMod = func(*Values) (any, error)
	AfterMod  = func(link any, retrieved any) error
)

func (m MapperMod) apply(o *mappingOptions) {
	o.mapperMods = append(o.mapperMods, m)
}

func Mod[T any](m Mapper[T], mods ...MapperMod) Mapper[T] {
	return func(ctx context.Context, c cols) (func(*Values) (any, error), func(any) (T, error)) {
		before, after := m(ctx, c)
		befores := make([]BeforeMod, len(mods))
		afters := make([]AfterMod, len(mods))
		links := make([]any, len(mods))
		for i, m := range mods {
			befores[i], afters[i] = m(ctx, c)
		}

		return func(v *Values) (any, error) {
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
