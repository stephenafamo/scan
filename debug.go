package scan

import (
	"context"
	"fmt"
)

func Debug(q Queryer) Queryer {
	return debugQueryer{w: q}
}

type debugQueryer struct {
	w Queryer
}

func (d debugQueryer) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	fmt.Println(query)
	fmt.Println([]any(args))
	return d.w.QueryContext(ctx, query, args...)
}
