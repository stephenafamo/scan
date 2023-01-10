package scan

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aarondl/opt"
	"github.com/google/go-cmp/cmp"
)

type (
	strstr = [][2]string
	rows   = [][]any
)

var (
	now       = time.Now()
	goodSlice = []any{
		now,
		100,
		"A string",
		sql.NullString{Valid: false},
		"another string",
		[]byte("interesting"),
	}
)

type stdQ struct {
	*sql.DB
}

func (s stdQ) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	return s.DB.QueryContext(ctx, query, args...)
}

type Timestamps struct {
	CreatedAt time.Time
	UpdatedAt time.Time
}

type PtrTimestamps struct {
	CreatedAt *time.Time
	UpdatedAt *time.Time
}

type User struct {
	ID   int
	Name string
}

type PtrUser1 struct {
	ID   *int
	Name string
	PtrTimestamps
}

type PtrUser2 struct {
	ID   int
	Name *string
	*PtrTimestamps
}

type UserWithTimestamps struct {
	User
	*Timestamps
	Blog *Blog
}

type Blog struct {
	ID   int
	User UserWithTimestamps
}

type Tagged struct {
	ID      int    `db:"tag_id" custom:"custom_id"`
	Name    string `db:"tag_name" custom:"custom_name"`
	Email   string `db:"EMAIL"`
	Exclude int    `db:"-" custom:"-"`
}

type ScannableUser struct {
	ID   int
	Name string
}

func (s ScannableUser) Scan() {
}

type wrapper struct {
	V any
}

// Scan implements the sql.Scanner interface. If the wrapped type implements
// sql.Scanner then it will call that.
func (v *wrapper) Scan(value any) error {
	if scanner, ok := v.V.(sql.Scanner); ok {
		return scanner.Scan(value)
	}

	if err := opt.ConvertAssign(v.V, value); err != nil {
		return fmt.Errorf("convert assign err: %w", err)
	}

	return nil
}

type typeConverter struct{}

func (d typeConverter) TypeToDestination(typ reflect.Type) reflect.Value {
	val := reflect.ValueOf(&wrapper{
		V: reflect.New(typ).Interface(),
	})

	return val
}

func (d typeConverter) ValueFromDestination(val reflect.Value) reflect.Value {
	return val.Elem().FieldByName("V").Elem().Elem()
}

func toPtr[T any](v T) *T {
	return &v
}

// To quickly generate column definition for tests
// make it in the form {"1": 1, "2": 2}
func columns(n int) []string {
	m := make([]string, n)
	for i := 0; i < n; i++ {
		m[i] = strconv.Itoa(i)
	}

	return m
}

func columnNames(names ...string) []string {
	return names
}

func colSliceFromMap(c [][2]string) []string {
	s := make([]string, 0, len(c))
	for _, def := range c {
		s = append(s, def[0])
	}
	return s
}

func singleRows[T any](vals ...T) rows {
	r := make(rows, len(vals))
	for k, v := range vals {
		r[k] = []any{v}
	}

	return r
}

func mapToVals[T any](vals []any) map[string]T {
	m := make(map[string]T, len(vals))
	for i, v := range vals {
		m[strconv.Itoa(i)] = v.(T)
	}

	return m
}

func randate() time.Time {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2070, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}

func userMod(ctx context.Context, c cols) (BeforeFunc, AfterMod) {
	return func(v *Row) (any, error) {
			return nil, nil
		}, func(link, retrieved any) error {
			u, ok := retrieved.(*User)
			if !ok {
				return errors.New("wrong retrieved type")
			}
			if u == nil {
				return nil
			}
			u.ID *= 200
			u.Name += " modified"

			return nil
		}
}

func convertMappingError(m *MappingError) string {
	return strings.Join(m.meta, " ")
}

func diffErr(expected, got error) string {
	return cmp.Diff(expected, got, cmp.Transformer("convertMappingErr", convertMappingError), equateErrors())
}

// equateErrors returns a Comparer option that determines errors to be equal
// if errors.Is reports them to match. The AnyError error can be used to
// match any non-nil error.
func equateErrors() cmp.Option {
	return cmp.FilterValues(nonMappingErrors, cmp.Comparer(compareErrors))
}

// nonMappingErrors reports whether x and y are types that implement error.
// The input types are deliberately of the interface{} type rather than the
// error type so that we can handle situations where the current type is an
// interface{}, but the underlying concrete types both happen to implement
// the error interface.
func nonMappingErrors(x, y error) bool {
	var me *MappingError
	ok1 := errors.As(x, &me)
	ok2 := errors.As(y, &me)
	return !(ok1 && ok2)
}

func compareErrors(xe, ye error) bool {
	if errors.Is(xe, ye) || errors.Is(ye, xe) {
		return true
	}

	return xe.Error() == ye.Error()
}
