package scan

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
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

type queryCase[T any] struct {
	ctx         context.Context
	columns     strstr
	rows        rows
	query       []string // columns to select
	mapper      Mapper[T]
	expectOne   T
	expectAll   []T
	expectedErr error
}

func testQuery[T any](t *testing.T, name string, tc queryCase[T]) {
	t.Helper()

	t.Run(name, func(t *testing.T) {
		ctx := tc.ctx
		if ctx == nil {
			ctx = context.Background()
		}

		ex, clean := createDB(t, tc.columns)
		defer clean()

		insert(t, ex, colSliceFromMap(tc.columns), tc.rows...)
		query := createQuery(t, tc.query)

		queryer := stdQ{ex}
		t.Run("one", func(t *testing.T) {
			one, err := One(ctx, queryer, tc.mapper, query)
			if diff := diffErr(tc.expectedErr, err); diff != "" {
				t.Fatalf("diff: %s", diff)
			}

			if diff := cmp.Diff(tc.expectOne, one); diff != "" {
				t.Fatalf("diff: %s", diff)
			}
		})

		t.Run("all", func(t *testing.T) {
			all, err := All(ctx, queryer, tc.mapper, query)
			if diff := diffErr(tc.expectedErr, err); diff != "" {
				t.Fatalf("diff: %s", diff)
			}

			if diff := cmp.Diff(tc.expectAll, all); diff != "" {
				t.Fatalf("diff: %s", diff)
			}
		})

		t.Run("each", func(t *testing.T) {
			var i int
			Each(ctx, queryer, tc.mapper, query)(func(val T, err error) bool {
				if diff := diffErr(tc.expectedErr, err); diff != "" {
					t.Fatalf("diff: %s", diff)
				}

				if err != nil {
					return false
				}

				if diff := cmp.Diff(tc.expectAll[i], val); diff != "" {
					t.Fatalf("diff: %s", diff)
				}
				i++
				return true
			})
			if i != len(tc.expectAll) {
				t.Fatalf("Should have %d rows, but each only scanned %d", len(tc.expectAll), i)
			}
		})

		t.Run("cursor", func(t *testing.T) {
			c, err := Cursor(ctx, queryer, tc.mapper, query)
			if err != nil {
				t.Fatalf("error getting cursor: %v", err)
				return
			}
			defer c.Close()

			var i int
			for c.Next() {
				v, err := c.Get()
				if diff := diffErr(tc.expectedErr, err); diff != "" {
					t.Fatalf("diff: %s", diff)
				}

				if err != nil {
					return
				}

				if diff := cmp.Diff(tc.expectAll[i], v); diff != "" {
					t.Fatalf("diff: %s", diff)
				}

				i++
			}

			if i != len(tc.expectAll) {
				t.Fatalf("Should have %d rows, but cursor only scanned %d", len(tc.expectAll), i)
			}

			if diff := diffErr(tc.expectedErr, c.Err()); diff != "" {
				t.Fatalf("diff: %s", diff)
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

func TestColumnValue(t *testing.T) {
	testQuery(t, "int", queryCase[int]{
		columns:   strstr{{"id", "int64"}},
		rows:      singleRows(1, 2, 3, 5, 8, 13, 21),
		query:     []string{"id"},
		mapper:    ColumnMapper[int]("id"),
		expectOne: 1,
		expectAll: []int{1, 2, 3, 5, 8, 13, 21},
	})

	testQuery(t, "unknown", queryCase[int]{
		columns:     strstr{{"id", "int64"}},
		rows:        singleRows(1, 2, 3, 5, 8, 13, 21),
		query:       []string{"id"},
		mapper:      ColumnMapper[int]("unknown_column"),
		expectedErr: createError(nil, "unknown_column"),
	})
}

func TestMap(t *testing.T) {
	user1 := map[string]any{"id": int64(1), "name": "foo"}
	user2 := map[string]any{"id": int64(2), "name": "bar"}

	testQuery(t, "user", queryCase[map[string]any]{
		columns:   strstr{{"id", "int64"}, {"name", "string"}},
		rows:      rows{[]any{1, "foo"}, []any{2, "bar"}},
		query:     []string{"id", "name"},
		mapper:    MapMapper[any],
		expectOne: user1,
		expectAll: []map[string]any{user1, user2},
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

	testQuery(t, "user with type converter", queryCase[User]{
		columns:   strstr{{"id", "int64"}, {"name", "string"}},
		rows:      rows{[]any{1, "foo"}, []any{2, "bar"}},
		query:     []string{"id", "name"},
		mapper:    StructMapper[User](WithTypeConverter(typeConverter{})),
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
		expectedErr: createError(nil, "no destination", "missing1"),
	})

	testQuery(t, "userWithMod", queryCase[*User]{
		columns:   strstr{{"id", "int64"}, {"name", "string"}},
		rows:      rows{[]any{1, "foo"}, []any{2, "bar"}},
		query:     []string{"id", "name"},
		mapper:    CustomStructMapper[*User](defaultStructMapper, WithMapperMods(userMod)),
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

func TestAllowUnknownColumns(t *testing.T) {
	type testStruct struct {
		ID  int64
		Int int64
	}

	// fails when context does not have CtxKeyAllowUnknownColumns set to true
	testQuery(t, "unknowncolumnsnotallowed", queryCase[testStruct]{
		columns:     strstr{{"id", "int64"}, {"ignored_int", "int64"}, {"int", "int64"}},
		rows:        rows{{1, 10, 1}, {2, 20, 2}},
		query:       []string{"id", "ignored_int", "int"},
		mapper:      StructMapper[testStruct](),
		expectedErr: createError(fmt.Errorf("No destination for column ignored_int"), "no destination", "ignored_int"),
	})

	// succeeds when context has CtxKeyAllowUnknownColumns set to true
	testQuery(t, "unknowncolumnsallowed", queryCase[testStruct]{
		ctx:       context.WithValue(context.Background(), CtxKeyAllowUnknownColumns, true),
		columns:   strstr{{"id", "int64"}, {"ignored_int", "int64"}, {"int", "int64"}},
		rows:      rows{{1, 10, 1}, {2, 20, 2}},
		query:     []string{"id", "ignored_int", "int"},
		mapper:    StructMapper[testStruct](),
		expectOne: testStruct{ID: 1, Int: 1},
		expectAll: []testStruct{{ID: 1, Int: 1}, {ID: 2, Int: 2}},
	})
}
