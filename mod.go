package scan

import "context"

type (
	MapperMod     = func(context.Context, cols) MapperModFunc
	MapperModFunc = func(*Values, any) error
)

func Mod[T any](m Mapper[T], mods ...MapperMod) Mapper[T] {
	return func(ctx context.Context, c cols) (func(*Values) error, func(*Values) (T, error)) {
		before, after := m(ctx, c)
		fs := make([]MapperModFunc, len(mods))
		for i, m := range mods {
			fs[i] = m(ctx, c)
		}

		return before, func(v *Values) (T, error) {
			t, err := after(v)
			if err != nil {
				return t, err
			}

			for _, f := range fs {
				if err := f(v, t); err != nil {
					return t, err
				}
			}

			return t, nil
		}
	}
}
