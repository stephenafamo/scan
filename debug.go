package scan

import (
	"context"
	"fmt"
	"io"
	"os"
)

func Debug(q Queryer, w io.Writer) Queryer {
	if w == nil {
		w = os.Stdout
	}

	return debugQueryer{w: w, q: q}
}

type debugQueryer struct {
	w io.Writer
	q Queryer
}

func (d debugQueryer) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	fmt.Fprintln(d.w, query)
	fmt.Fprintln(d.w, []any(args))
	return d.q.QueryContext(ctx, query, args...)
}
