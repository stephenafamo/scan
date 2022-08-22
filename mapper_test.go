package scan

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func toPtr[T any](v T) *T {
	return &v
}

type MapperTests[T any] map[string]MapperTest[T]

type MapperTest[T any] struct {
	Values           *Values
	Prefix           string
	AllowUnknownCols bool
	Mapper           Mapper[T]
	ExpectedVal      T
	ExpectedError    error
}

// To quickly generate column definition for tests
// make it in the form {"1": 1, "2": 2}
func columns(n int) map[string]int {
	m := make(map[string]int, n)
	for i := 0; i < n; i++ {
		m[strconv.Itoa(i)] = i
	}

	return m
}

func columnNames(names ...string) map[string]int {
	m := make(map[string]int, len(names))
	for i, name := range names {
		m[name] = i
	}

	return m
}

func RunMapperTests[T any](t *testing.T, cases MapperTests[T]) {
	t.Helper()
	for name, tc := range cases {
		RunMapperTest(t, name, tc)
	}
}

func RunMapperTest[T any](t *testing.T, name string, tc MapperTest[T]) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		ctx := context.Background()
		if tc.Prefix != "" {
			ctx = context.WithValue(ctx, CtxKeyStructTagPrefix, tc.Prefix)
		}
		if tc.AllowUnknownCols {
			ctx = context.WithValue(ctx, CtxKeyAllowUnknownColumns, tc.AllowUnknownCols)
		}

		m := tc.Mapper(ctx, tc.Values.columnsCopy())

		val, err := m(tc.Values)
		if diff := cmp.Diff(tc.ExpectedError, err); diff != "" {
			t.Fatalf("diff: %s", diff)
		}
		if diff := cmp.Diff(tc.ExpectedVal, val); diff != "" {
			t.Fatalf("diff: %s", diff)
		}
	})
}

func TestColumnMapper(t *testing.T) {
	RunMapperTest(t, "single column", MapperTest[int]{
		Values: &Values{
			columns: columns(1),
			scanned: []any{100},
		},
		Mapper:      ColumnMapper[int]("0"),
		ExpectedVal: 100,
	})

	RunMapperTest(t, "multiple columns", MapperTest[int]{
		Values: &Values{
			columns: columns(3),
			scanned: []any{100, 200, 300},
		},
		Mapper:      ColumnMapper[int]("1"),
		ExpectedVal: 200,
	})
}

func TestSingleColumnMapper(t *testing.T) {
	RunMapperTest(t, "multiple columns", MapperTest[int]{
		Values: &Values{
			columns: columns(2),
			scanned: []any{100},
		},
		Mapper:        SingleColumnMapper[int],
		ExpectedError: createError(nil, "wrong column count", "1", "2"),
	})

	RunMapperTest(t, "int", MapperTest[int]{
		Values: &Values{
			columns: columns(1),
			scanned: []any{100},
		},
		Mapper:      SingleColumnMapper[int],
		ExpectedVal: 100,
	})

	RunMapperTest(t, "int64", MapperTest[int64]{
		Values: &Values{
			columns: columns(1),
			scanned: []any{int64(100)},
		},
		Mapper:      SingleColumnMapper[int64],
		ExpectedVal: 100,
	})

	RunMapperTest(t, "string", MapperTest[string]{
		Values: &Values{
			columns: columns(1),
			scanned: []any{"A fancy string"},
		},
		Mapper:      SingleColumnMapper[string],
		ExpectedVal: "A fancy string",
	})

	RunMapperTest(t, "time.Time", MapperTest[time.Time]{
		Values: &Values{
			columns: columns(1),
			scanned: []any{now},
		},
		Mapper:      SingleColumnMapper[time.Time],
		ExpectedVal: now,
	})
}

var goodSlice = []any{
	now,
	100,
	"A string",
	sql.NullString{Valid: false},
	"another string",
	[]byte("interesting"),
}

func TestSliceMapper(t *testing.T) {
	RunMapperTest(t, "any slice", MapperTest[[]any]{
		Values: &Values{
			columns: columns(len(goodSlice)),
			scanned: goodSlice,
		},
		Mapper:      SliceMapper[any],
		ExpectedVal: goodSlice,
	})

	RunMapperTest(t, "int slice", MapperTest[[]int]{
		Values: &Values{
			columns: columns(1),
			scanned: []any{100},
		},
		Mapper:      SliceMapper[int],
		ExpectedVal: []int{100},
	})
}

func mapToVals[T any](vals []any) map[string]T {
	m := make(map[string]T, len(vals))
	for i, v := range vals {
		m[strconv.Itoa(i)] = v.(T)
	}

	return m
}

func TestMapMapper(t *testing.T) {
	RunMapperTest(t, "MapMapper", MapperTest[map[string]any]{
		Values: &Values{
			columns: columns(len(goodSlice)),
			scanned: goodSlice,
		},
		Mapper:      MapMapper[any],
		ExpectedVal: mapToVals[any](goodSlice),
	})
}

func TestStructMapper(t *testing.T) {
	RunMapperTest(t, "No destination", MapperTest[User]{
		Values: &Values{
			columns: columnNames("random"),
		},
		Mapper:        StructMapper[User],
		ExpectedError: createError(nil, "no destination", "random"),
	})

	RunMapperTest(t, "Unknown cols permitted with custom mapper", MapperTest[*User]{
		Values: &Values{
			columns: columnNames("random"),
		},
		Mapper:      CustomStructMapper[*User](WithAllowUnknownColumns(true)),
		ExpectedVal: &User{},
	})

	RunMapperTest(t, "Unknown cols permitted with context", MapperTest[User]{
		Values: &Values{
			columns: columnNames("random"),
		},
		Mapper:           StructMapper[User],
		AllowUnknownCols: true,
		ExpectedVal:      User{},
	})

	RunMapperTest(t, "flat struct", MapperTest[User]{
		Values: &Values{
			columns: columnNames("id", "name"),
			scanned: []any{1, "The Name"},
		},
		Mapper:      StructMapper[User],
		ExpectedVal: User{ID: 1, Name: "The Name"},
	})

	RunMapperTest(t, "with pointer columns 1", MapperTest[PtrUser1]{
		Values: &Values{
			columns: columnNames("id", "name", "created_at", "updated_at"),
			scanned: []any{1, "The Name", now, now.Add(time.Hour)},
		},
		Mapper: StructMapper[PtrUser1],
		ExpectedVal: PtrUser1{
			ID: toPtr(1), Name: "The Name",
			PtrTimestamps: PtrTimestamps{CreatedAt: &now, UpdatedAt: toPtr(now.Add(time.Hour))},
		},
	})

	RunMapperTest(t, "with pointer columns 2", MapperTest[PtrUser2]{
		Values: &Values{
			columns: columnNames("id", "name", "created_at", "updated_at"),
			scanned: []any{1, "The Name", now, now.Add(time.Hour)},
		},
		Mapper: StructMapper[PtrUser2],
		ExpectedVal: PtrUser2{
			ID: 1, Name: toPtr("The Name"),
			PtrTimestamps: &PtrTimestamps{CreatedAt: &now, UpdatedAt: toPtr(now.Add(time.Hour))},
		},
	})

	RunMapperTest(t, "anonymous embeds", MapperTest[UserWithTimestamps]{
		Values: &Values{
			columns: columnNames("id", "name", "created_at", "updated_at"),
			scanned: []any{10, "The Name", now, now.Add(time.Hour)},
		},
		Mapper: StructMapper[UserWithTimestamps],
		ExpectedVal: UserWithTimestamps{
			User:       User{ID: 10, Name: "The Name"},
			Timestamps: &Timestamps{CreatedAt: now, UpdatedAt: now.Add(time.Hour)},
		},
	})

	RunMapperTest(t, "prefixed structs", MapperTest[Blog]{
		Values: &Values{
			columns: columnNames("id", "user.id", "user.name", "user.created_at"),
			scanned: []any{100, 10, "The Name", now},
		},
		Mapper: StructMapper[Blog],
		ExpectedVal: Blog{
			ID: 100,
			User: UserWithTimestamps{
				User:       User{ID: 10, Name: "The Name"},
				Timestamps: &Timestamps{CreatedAt: now},
			},
		},
	})

	RunMapperTest(t, "tagged", MapperTest[Tagged]{
		Values: &Values{
			columns: columnNames("tag_id", "tag_name"),
			scanned: []any{1, "The Name"},
		},
		Mapper:      StructMapper[Tagged],
		ExpectedVal: Tagged{ID: 1, Name: "The Name"},
	})

	RunMapperTest(t, "allow unknown columns", MapperTest[*User]{
		Values: &Values{
			columns: columnNames("random"),
		},
		Mapper:      CustomStructMapper[*User](WithAllowUnknownColumns(true)),
		ExpectedVal: &User{},
	})

	RunMapperTest(t, "custom column separator", MapperTest[Blog]{
		Values: &Values{
			columns: columnNames("id", "user,id", "user,name", "user,created_at"),
			scanned: []any{100, 10, "The Name", now},
		},
		Mapper: CustomStructMapper[Blog](WithColumnSeparator(",")),
		ExpectedVal: Blog{
			ID: 100,
			User: UserWithTimestamps{
				User:       User{ID: 10, Name: "The Name"},
				Timestamps: &Timestamps{CreatedAt: now},
			},
		},
	})

	RunMapperTest(t, "custom name mapper", MapperTest[Blog]{
		Values: &Values{
			columns: columnNames("ID", "USER.ID", "USER.NAME", "USER.CREATEDAT"),
			scanned: []any{100, 10, "The Name", now},
		},
		Mapper: CustomStructMapper[Blog](WithFieldNameMapper(strings.ToUpper)),
		ExpectedVal: Blog{
			ID: 100,
			User: UserWithTimestamps{
				User:       User{ID: 10, Name: "The Name"},
				Timestamps: &Timestamps{CreatedAt: now},
			},
		},
	})

	RunMapperTest(t, "custom tag", MapperTest[Tagged]{
		Values: &Values{
			columns: columnNames("custom_id", "custom_name"),
			scanned: []any{1, "The Name"},
		},
		Mapper:      CustomStructMapper[Tagged](WithStructTagKey("custom")),
		ExpectedVal: Tagged{ID: 1, Name: "The Name"},
	})

	RunMapperTest(t, "custom tag", MapperTest[UserWithMapper]{
		Values: &Values{
			columns: columnNames("id", "name"),
			scanned: []any{1, "The Name"},
		},
		Mapper:      StructMapper[UserWithMapper],
		ExpectedVal: UserWithMapper{ID: 100, Name: "@The Name"},
	})

	RunMapperTest(t, "with prefix", MapperTest[User]{
		Values: &Values{
			columns: columnNames("prefix--id", "prefix--name"),
			scanned: []any{1, "The Name"},
		},
		Mapper:      StructMapper[User],
		Prefix:      "prefix--",
		ExpectedVal: User{ID: 1, Name: "The Name"},
	})

	RunMapperTest(t, "with prefix and non-prefixed column", MapperTest[User]{
		Values: &Values{
			columns: columnNames("id", "prefix--name"),
			scanned: []any{1, "The Name"},
		},
		Mapper:      StructMapper[User],
		Prefix:      "prefix--",
		ExpectedVal: User{ID: 0, Name: "The Name"},
	})

	RunMapperTest(t, "with mod", MapperTest[*User]{
		Values: &Values{
			columns: columnNames("id", "name"),
			scanned: []any{2, "The Name"},
		},
		Mapper: Mod(StructMapper[*User], func(ctx context.Context, c cols) MapperModFunc {
			return func(v *Values, o any) error {
				u, ok := o.(*User)
				if !ok {
					return errors.New("wrong retrieved type")
				}
				u.ID *= 200
				u.Name += " modified"
				return nil
			}
		}),
		ExpectedVal: &User{ID: 400, Name: "The Name modified"},
	})
}

func TestAggStructMapper(t *testing.T) {
	RunMapperTest(t, "flat struct", MapperTest[[]User]{
		Values: &Values{
			columns: columnNames("id", "name"),
			scanned: []any{[]int{1, 2}, []string{"The Name", "Another Name"}},
		},
		Mapper: AggStructMapper[User, []User](defaultConverter{}),
		ExpectedVal: []User{
			{ID: 1, Name: "The Name"},
			{ID: 2, Name: "Another Name"},
		},
	})
}

type defaultConverter struct{}

func (d defaultConverter) ConvertToAgg(typ reflect.Type) reflect.Value {
	return reflect.New(reflect.SliceOf(typ))
}

func (d defaultConverter) ConvertFromAgg(val reflect.Value) reflect.Value {
	return val
}

func TestMappable(t *testing.T) {
	testMappable[noMatchingMethod](t, false)
	testMappable[methodWithWrongSignature](t, false)

	testMappable[mappableVV](t, true)
	testMappable[*mappableVV](t, true)

	testMappable[mappableVP](t, true)
	testMappable[*mappableVP](t, true)

	testMappable[mappablePV](t, true)
	testMappable[*mappablePV](t, true)

	testMappable[mappablePP](t, true)
	testMappable[*mappablePP](t, true)
}

func testMappable[T any](t *testing.T, expected bool) {
	t.Helper()

	var x T
	typ := reflect.TypeOf(x)

	t.Run(typ.String(), func(t *testing.T) {
		f, gotten := mappable[T](typ, typ.Kind() == reflect.Pointer)
		if expected != gotten {
			t.Fatalf("Expected mappable(%T) to be %t but was %t", x, expected, gotten)
		}
		if gotten {
			f(context.Background(), nil)
		}
	})
}

var now = time.Now()

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
	ID   int    `db:"tag_id" custom:"custom_id"`
	Name string `db:"tag_name" custom:"custom_name"`
}

type UserWithMapper struct {
	ID   int
	Name string
}

func (UserWithMapper) MapValues(context.Context, cols) func(*Values) (UserWithMapper, error) {
	return func(v *Values) (UserWithMapper, error) {
		return UserWithMapper{
			ID:   Value[int](v, "id") * 100,
			Name: "@" + Value[string](v, "name"),
		}, nil
	}
}

type noMatchingMethod struct{}

type methodWithWrongSignature struct{}

func (methodWithWrongSignature) MapValues(cols) func(methodWithWrongSignature, error) {
	return nil
}

// value to value
type mappableVV struct{}

func (mappableVV) MapValues(context.Context, cols) func(*Values) (mappableVV, error) {
	return nil
}

// value to pointer
type mappableVP struct{}

func (mappableVP) MapValues(context.Context, cols) func(*Values) (*mappableVP, error) {
	return nil
}

// pointer to value
type mappablePV struct{}

func (*mappablePV) MapValues(context.Context, cols) func(*Values) (mappablePV, error) {
	return nil
}

// pointer to pointer
type mappablePP struct{}

func (*mappablePP) MapValues(context.Context, cols) func(*Values) (*mappablePP, error) {
	return nil
}
