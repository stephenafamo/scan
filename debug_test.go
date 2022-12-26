package scan

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
)

type NoopQueryer struct{}

func (n NoopQueryer) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	return nil, nil
}

func TestDebugQueryerDefaultWriter(t *testing.T) {
	d, ok := Debug(NoopQueryer{}, nil).(debugQueryer)
	if !ok {
		t.Fatal("DebugQueryer does not return an instance of debugQueryer")
	}

	debugFile, ok := d.w.(*os.File)
	if !ok {
		t.Fatal("writer for debugQueryer is not an *os.File")
	}

	if debugFile != os.Stdout {
		t.Fatal("writer for debugQueryer is not os.Stdout")
	}
}

func TestDebugQueryer(t *testing.T) {
	dest := &bytes.Buffer{}
	exec := Debug(NoopQueryer{}, dest)

	sql := "A QUERY"
	args := []any{"arg1", "arg2", "arg3"}

	_, err := exec.QueryContext(context.Background(), sql, args...)
	if err != nil {
		t.Fatal("error running QueryContext")
	}

	debugsql, debugArgsStr, found := strings.Cut(dest.String(), "\n")
	if !found {
		t.Fatalf("arg delimiter not found in\n%s", dest.String())
	}

	if strings.TrimSpace(debugsql) != sql {
		t.Fatalf("wrong debug sql.\nExpected: %s\nGot: %s", sql, strings.TrimSpace(debugsql))
	}

	debugArgsStr = strings.TrimSpace(debugArgsStr)
	debugArgsStr = strings.Trim(debugArgsStr, "[]")

	var debugArgs []string //nolint:prealloc
	for _, s := range strings.Fields(debugArgsStr) {
		s := strings.TrimSpace(s)
		if s == "" {
			continue
		}

		debugArgs = append(debugArgs, s)
	}

	if len(debugArgs) != len(args) {
		t.Fatalf("wrong length of debug args.\nExpected: %d\nGot: %d\n\n%s", len(args), len(debugArgs), debugArgs)
	}

	for i := range args {
		argStr := strings.TrimSpace(fmt.Sprint(args[i]))
		debugStr := strings.TrimSpace(debugArgs[i])
		if argStr != debugStr {
			t.Fatalf("wrong debug arg %d.\nExpected: %s\nGot: %s", i, argStr, debugStr)
		}
	}
}
