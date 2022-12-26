package scan

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	_ "github.com/stephenafamo/fakedb"
)

func createDB(tb testing.TB, cols [][2]string) (*sql.DB, func()) {
	tb.Helper()
	db, err := sql.Open("test", "foo")
	if err != nil {
		tb.Fatalf("Error opening testdb %v", err)
	}

	first := true
	b := &strings.Builder{}
	fmt.Fprintf(b, "CREATE|%s|", tb.Name())

	for _, def := range cols {
		if !first {
			b.WriteString(",")
		} else {
			first = false
		}

		fmt.Fprintf(b, "%s=%s", def[0], def[1])
	}

	exec(tb, db, b.String())
	return db, func() {
		exec(tb, db, fmt.Sprintf("DROP|%s", tb.Name()))
	}
}

func exec(tb testing.TB, exec *sql.DB, query string, args ...interface{}) sql.Result {
	tb.Helper()
	result, err := exec.ExecContext(context.Background(), query, args...)
	if err != nil {
		tb.Fatalf("Exec of %q: %v", query, err)
	}

	return result
}

func insert(tb testing.TB, ex *sql.DB, cols []string, vals ...[]any) {
	tb.Helper()
	query := fmt.Sprintf("INSERT|%s|%s=?", tb.Name(), strings.Join(cols, "=?,"))
	for _, val := range vals {
		exec(tb, ex, query, val...)
	}
}

func createQuery(tb testing.TB, cols []string) string {
	tb.Helper()
	return fmt.Sprintf("SELECT|%s|%s|", tb.Name(), strings.Join(cols, ","))
}

type (
	strstr = [][2]string
	rows   = [][]any
)

type queryCase[T any] struct {
	columns     strstr
	rows        rows
	query       []string // columns to select
	mapper      Mapper[T]
	mapperMods  []MapperMod
	expectOne   T
	expectAll   []T
	expectedErr error
}

func testQuery[T any](t *testing.T, name string, tc queryCase[T]) {
	t.Helper()

	t.Run(name, func(t *testing.T) {
		ctx := context.Background()
		if len(tc.mapperMods) > 0 {
			tc.mapper = Mod(tc.mapper, tc.mapperMods...)
		}

		ex, clean := createDB(t, tc.columns)
		defer clean()

		insert(t, ex, colSliceFromMap(tc.columns), tc.rows...)
		query := createQuery(t, tc.query)

		queryer := stdQ{ex}
		t.Run("one", func(t *testing.T) {
			one, err := One(ctx, queryer, tc.mapper, query)
			if diff := cmp.Diff(tc.expectedErr, err, cmp.Comparer(compareMappingError)); diff != "" {
				t.Fatalf("diff: %s", diff)
			}

			if diff := cmp.Diff(tc.expectOne, one); diff != "" {
				t.Fatalf("diff: %s", diff)
			}
		})

		t.Run("all", func(t *testing.T) {
			all, err := All(ctx, queryer, tc.mapper, query)
			if diff := cmp.Diff(tc.expectedErr, err, cmp.Comparer(compareMappingError)); diff != "" {
				t.Fatalf("diff: %s", diff)
			}

			if diff := cmp.Diff(tc.expectAll, all); diff != "" {
				t.Fatalf("diff: %s", diff)
			}
		})

		t.Run("cursor", func(t *testing.T) {
			c, err := Cursor(ctx, queryer, tc.mapper, query)
			if diff := cmp.Diff(tc.expectedErr, err, cmp.Comparer(compareMappingError)); diff != "" {
				t.Fatalf("diff: %s", diff)
			}

			if err != nil {
				return
			}

			var i int
			for c.Next() {
				v, err := c.Get()
				if diff := cmp.Diff(tc.expectedErr, err, cmp.Comparer(compareMappingError)); diff != "" {
					t.Fatalf("diff: %s", diff)
				}

				if diff := cmp.Diff(tc.expectAll[i], v); diff != "" {
					t.Fatalf("diff: %s", diff)
				}

				i++
			}

			if i != len(tc.expectAll) {
				t.Fatalf("Should have %d rows, but cursor only scanned %d", len(tc.expectAll), i)
			}
		})
	})
}

func TestSingleValue(t *testing.T) {
	testQuery(t, "int", queryCase[int]{
		columns:   strstr{{"id", "int64"}},
		rows:      singleRows(1, 2, 3, 5, 8, 13, 21),
		query:     []string{"id"},
		mapper:    SingleColumnMapper[int],
		expectOne: 1,
		expectAll: []int{1, 2, 3, 5, 8, 13, 21},
	})

	testQuery(t, "string", queryCase[string]{
		columns:   strstr{{"name", "string"}},
		rows:      singleRows("first", "second", "third"),
		query:     []string{"name"},
		mapper:    SingleColumnMapper[string],
		expectOne: "first",
		expectAll: []string{"first", "second", "third"},
	})

	time1 := randate()
	time2 := randate()
	time3 := randate()
	testQuery(t, "datetime", queryCase[time.Time]{
		columns:   strstr{{"when", "datetime"}},
		rows:      singleRows(time1, time2, time3),
		query:     []string{"when"},
		mapper:    SingleColumnMapper[time.Time],
		expectOne: time1,
		expectAll: []time.Time{time1, time2, time3},
	})
}

func TestStruct(t *testing.T) {
	user1 := User{ID: 1, Name: "foo"}
	user2 := User{ID: 2, Name: "bar"}

	testQuery(t, "user", queryCase[User]{
		columns:   strstr{{"id", "int64"}, {"name", "string"}},
		rows:      rows{[]any{1, "foo"}, []any{2, "bar"}},
		query:     []string{"id", "name"},
		mapper:    StructMapper[User](),
		expectOne: user1,
		expectAll: []User{user1, user2},
	})

	testQuery(t, "user with unknown column", queryCase[User]{
		columns: strstr{
			{"id", "int64"},
			{"name", "string"},
			{"missing1", "int64"},
			{"missing2", "string"},
		},
		rows:        rows{[]any{1, "foo", "100", "foobar"}, []any{2, "bar", "200", "barfoo"}},
		query:       []string{"id", "name", "missing1", "missing2"},
		mapper:      StructMapper[User](),
		expectedErr: createError(nil, "no destination", "missing1", "missing2"),
	})

	testQuery(t, "userWithMod", queryCase[*User]{
		columns: strstr{{"id", "int64"}, {"name", "string"}},
		rows:    rows{[]any{1, "foo"}, []any{2, "bar"}},
		query:   []string{"id", "name"},
		mapper:  StructMapper[*User](),
		mapperMods: []MapperMod{
			func(ctx context.Context, c cols) MapperModFunc {
				return func(v *Values, o any) error {
					u, ok := o.(*User)
					if !ok {
						return errors.New("wrong retrieved type")
					}
					u.ID *= 200
					u.Name += " modified"
					return nil
				}
			},
		},
		expectOne: &User{ID: 200, Name: "foo modified"},
		expectAll: []*User{
			{ID: 200, Name: "foo modified"},
			{ID: 400, Name: "bar modified"},
		},
	})

	createdAt1 := randate()
	createdAt2 := randate()
	updatedAt1 := randate()
	updatedAt2 := randate()
	timestamp1 := &Timestamps{CreatedAt: createdAt1, UpdatedAt: updatedAt1}
	timestamp2 := &Timestamps{CreatedAt: createdAt2, UpdatedAt: updatedAt2}

	testQuery(t, "userwithtimestamps", queryCase[UserWithTimestamps]{
		columns: strstr{
			{"id", "int64"},
			{"name", "string"},
			{"created_at", "datetime"},
			{"updated_at", "datetime"},
		},
		rows: rows{
			[]any{1, "foo", createdAt1, updatedAt1},
			[]any{2, "bar", createdAt2, updatedAt2},
		},
		query:     []string{"id", "name", "created_at", "updated_at"},
		mapper:    StructMapper[UserWithTimestamps](),
		expectOne: UserWithTimestamps{User: user1, Timestamps: timestamp1},
		expectAll: []UserWithTimestamps{
			{User: user1, Timestamps: timestamp1},
			{User: user2, Timestamps: timestamp2},
		},
	})
}

func TestCollect(t *testing.T) {
	testCollect(t, "bad return", collectCase{
		columns:     strstr{{"id", "int64"}, {"name", "string"}},
		rows:        rows{[]any{1, "foo"}, []any{2, "bar"}},
		query:       []string{"id", "name"},
		generator:   func(_ context.Context, c cols) any { return 100 },
		expectedErr: ErrBadCollectorReturn,
	})

	testCollect(t, "bad input", collectCase{
		columns: strstr{{"id", "int64"}, {"name", "string"}},
		rows:    rows{[]any{1, "foo"}, []any{2, "bar"}},
		query:   []string{"id", "name"},
		generator: func(_ context.Context, c cols) any {
			return func(v Values) (int, string, error) {
				return 0, "", nil
			}
		},
		expectedErr: ErrBadCollectFuncInput,
	})

	testCollect(t, "bad output", collectCase{
		columns: strstr{{"id", "int64"}, {"name", "string"}},
		rows:    rows{[]any{1, "foo"}, []any{2, "bar"}},
		query:   []string{"id", "name"},
		generator: func(_ context.Context, c cols) any {
			return func(v *Values) error {
				return nil
			}
		},
		expectedErr: ErrBadCollectFuncOutput,
	})

	testCollect(t, "good", collectCase{
		columns: strstr{{"id", "int64"}, {"name", "string"}},
		rows:    rows{[]any{1, "foo"}, []any{2, "bar"}},
		query:   []string{"id", "name"},
		generator: func(_ context.Context, c cols) any {
			return func(v *Values) (int, string, error) {
				return Value[int](v, "id"), Value[string](v, "name"), nil
			}
		},
		expect: []any{[]int{1, 2}, []string{"foo", "bar"}},
	})

	testCollect(t, "good with struct", collectCase{
		columns: strstr{{"id", "int64"}, {"name", "string"}},
		rows:    rows{[]any{1, "foo"}, []any{2, "bar"}},
		query:   []string{"id", "name"},
		generator: func(ctx context.Context, c cols) any {
			sm := StructMapper[User]()(ctx, c)
			return func(v *Values) (int, User, error) {
				user, err := sm(v)
				return Value[int](v, "id"), user, err
			}
		},
		expect: []any{[]int{1, 2}, []User{
			{ID: 1, Name: "foo"},
			{ID: 2, Name: "bar"},
		}},
	})
}

type collectCase struct {
	columns     strstr
	rows        rows
	query       []string // columns to select
	generator   func(context.Context, cols) any
	expect      []any
	expectedErr error
}

func testCollect(t *testing.T, name string, tc collectCase) {
	t.Helper()
	ctx := context.Background()

	t.Run(name, func(t *testing.T) {
		ex, clean := createDB(t, tc.columns)
		defer clean()

		insert(t, ex, colSliceFromMap(tc.columns), tc.rows...)
		query := createQuery(t, tc.query)

		queryer := stdQ{ex}

		collected, err := Collect(ctx, queryer, tc.generator, query)
		if diff := cmp.Diff(tc.expectedErr, err, cmpopts.EquateErrors()); diff != "" {
			t.Fatalf("diff: %s", diff)
		}

		if diff := cmp.Diff(tc.expect, collected); diff != "" {
			t.Fatalf("diff: %s", diff)
		}
	})
}

type stdQ struct {
	*sql.DB
}

func (s stdQ) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	return s.DB.QueryContext(ctx, query, args...)
}
