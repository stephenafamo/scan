package scan

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

type MapperTests[T any] map[string]MapperTest[T]

type MapperTest[T any] struct {
	row                 *Row
	scanned             []any
	Context             map[contextKey]any
	Mapper              Mapper[T]
	ExpectedVal         T
	ExpectedBeforeError error
	ExpectedAfterError  error
}

func RunMapperTests[T any](t *testing.T, cases MapperTests[T]) {
	t.Helper()
	for name, tc := range cases {
		RunMapperTest(t, name, tc)
	}
}

func RunMapperTest[T any](t *testing.T, name string, tc MapperTest[T]) {
	t.Helper()

	f := func(t *testing.T) {
		t.Helper()
		ctx := context.Background()
		for k, v := range tc.Context {
			ctx = context.WithValue(ctx, k, v)
		}

		tc.row.scanDestinations = make([]reflect.Value, len(tc.row.columns))

		before, after := tc.Mapper(ctx, tc.row.columnsCopy())

		link, err := before(tc.row)
		if diff := diffErr(tc.ExpectedBeforeError, err); diff != "" {
			t.Fatalf("diff: %s", diff)
		}

		for i, ref := range tc.row.scanDestinations {
			if ref == zeroValue {
				continue
			}
			ref.Elem().Set(reflect.ValueOf(tc.scanned[i]))
		}

		val, err := after(link)
		if diff := diffErr(tc.ExpectedAfterError, err); diff != "" {
			t.Fatalf("diff: %s", diff)
		}
		if diff := cmp.Diff(tc.ExpectedVal, val); diff != "" {
			t.Fatalf("diff: %s", diff)
		}
	}

	if name == "" {
		f(t)
	} else {
		t.Run(name, f)
	}
}

type CustomStructMapperTest[T any] struct {
	MapperTest[T]
	Options []MappingSourceOption
}

func RunCustomStructMapperTest[T any](t *testing.T, name string, tc CustomStructMapperTest[T]) {
	t.Helper()
	m := tc.MapperTest
	src, err := NewStructMapperSource(tc.Options...)
	if diff := cmp.Diff(tc.ExpectedBeforeError, err); diff != "" {
		t.Fatalf("diff: %s", diff)
	}
	if err != nil {
		return
	}

	m.Mapper = CustomStructMapper[T](src)
	RunMapperTest(t, name, m)
}

func TestColumnMapper(t *testing.T) {
	RunMapperTest(t, "single column", MapperTest[int]{
		row: &Row{
			columns: columns(1),
		},
		scanned:     []any{100},
		Mapper:      ColumnMapper[int]("0"),
		ExpectedVal: 100,
	})

	RunMapperTest(t, "multiple columns", MapperTest[int]{
		row: &Row{
			columns: columns(3),
		},
		scanned:     []any{100, 200, 300},
		Mapper:      ColumnMapper[int]("1"),
		ExpectedVal: 200,
	})
}

func TestSingleColumnMapper(t *testing.T) {
	RunMapperTest(t, "multiple columns", MapperTest[int]{
		row: &Row{
			columns: columns(2),
		},
		scanned:             []any{100},
		Mapper:              SingleColumnMapper[int],
		ExpectedBeforeError: createError(nil, "wrong column count", "1", "2"),
		ExpectedAfterError:  createError(nil, "wrong column count", "1", "2"),
	})

	RunMapperTest(t, "int", MapperTest[int]{
		row: &Row{
			columns: columns(1),
		},
		scanned:     []any{100},
		Mapper:      SingleColumnMapper[int],
		ExpectedVal: 100,
	})

	RunMapperTest(t, "int64", MapperTest[int64]{
		row: &Row{
			columns: columns(1),
		},
		scanned:     []any{int64(100)},
		Mapper:      SingleColumnMapper[int64],
		ExpectedVal: 100,
	})

	RunMapperTest(t, "string", MapperTest[string]{
		row: &Row{
			columns: columns(1),
		},
		scanned:     []any{"A fancy string"},
		Mapper:      SingleColumnMapper[string],
		ExpectedVal: "A fancy string",
	})

	RunMapperTest(t, "time.Time", MapperTest[time.Time]{
		row: &Row{
			columns: columns(1),
		},
		scanned:     []any{now},
		Mapper:      SingleColumnMapper[time.Time],
		ExpectedVal: now,
	})
}

func TestSliceMapper(t *testing.T) {
	RunMapperTest(t, "any slice", MapperTest[[]any]{
		row: &Row{
			columns: columns(len(goodSlice)),
		},
		scanned:     goodSlice,
		Mapper:      SliceMapper[any],
		ExpectedVal: goodSlice,
	})

	RunMapperTest(t, "int slice", MapperTest[[]int]{
		row: &Row{
			columns: columns(1),
		},
		scanned:     []any{100},
		Mapper:      SliceMapper[int],
		ExpectedVal: []int{100},
	})
}

func TestMapMapper(t *testing.T) {
	RunMapperTest(t, "MapMapper", MapperTest[map[string]any]{
		row: &Row{
			columns: columns(len(goodSlice)),
		},
		scanned:     goodSlice,
		Mapper:      MapMapper[any],
		ExpectedVal: mapToVals[any](goodSlice),
	})
}

func TestStructMapper(t *testing.T) {
	RunMapperTest(t, "Unknown cols permitted", MapperTest[User]{
		row: &Row{
			columns: columnNames("random"),
		},
		Mapper:      StructMapper[User](),
		Context:     map[contextKey]any{CtxKeyAllowUnknownColumns: true},
		ExpectedVal: User{},
	})

	RunMapperTest(t, "flat struct", MapperTest[User]{
		row: &Row{
			columns: columnNames("id", "name"),
		},
		scanned:     []any{1, "The Name"},
		Mapper:      StructMapper[User](),
		ExpectedVal: User{ID: 1, Name: "The Name"},
	})

	RunMapperTest(t, "with pointer columns 1", MapperTest[PtrUser1]{
		row: &Row{
			columns: columnNames("id", "name", "created_at", "updated_at"),
		},
		scanned: []any{toPtr(1), "The Name", &now, toPtr(now.Add(time.Hour))},
		Mapper:  StructMapper[PtrUser1](),
		ExpectedVal: PtrUser1{
			ID: toPtr(1), Name: "The Name",
			PtrTimestamps: PtrTimestamps{CreatedAt: &now, UpdatedAt: toPtr(now.Add(time.Hour))},
		},
	})

	RunMapperTest(t, "with pointer columns 2", MapperTest[PtrUser2]{
		row: &Row{
			columns: columnNames("id", "name", "created_at", "updated_at"),
		},
		scanned: []any{1, toPtr("The Name"), &now, toPtr(now.Add(time.Hour))},
		Mapper:  StructMapper[PtrUser2](),
		ExpectedVal: PtrUser2{
			ID: 1, Name: toPtr("The Name"),
			PtrTimestamps: &PtrTimestamps{CreatedAt: &now, UpdatedAt: toPtr(now.Add(time.Hour))},
		},
	})

	RunMapperTest(t, "anonymous embeds", MapperTest[UserWithTimestamps]{
		row: &Row{
			columns: columnNames("id", "name", "created_at", "updated_at"),
		},
		scanned: []any{10, "The Name", now, now.Add(time.Hour)},
		Mapper:  StructMapper[UserWithTimestamps](),
		ExpectedVal: UserWithTimestamps{
			User:       User{ID: 10, Name: "The Name"},
			Timestamps: &Timestamps{CreatedAt: now, UpdatedAt: now.Add(time.Hour)},
		},
	})

	RunMapperTest(t, "prefixed structs", MapperTest[Blog]{
		row: &Row{
			columns: columnNames("id", "user.id", "user.name", "user.created_at"),
		},
		scanned: []any{100, 10, "The Name", now},
		Mapper:  StructMapper[Blog](),
		ExpectedVal: Blog{
			ID: 100,
			User: UserWithTimestamps{
				User:       User{ID: 10, Name: "The Name"},
				Timestamps: &Timestamps{CreatedAt: now},
			},
		},
	})

	RunMapperTest(t, "tagged", MapperTest[Tagged]{
		row: &Row{
			columns: columnNames("tag_id", "tag_name", "EMAIL"),
		},
		scanned:     []any{1, "The Name", "user@example.com"},
		Mapper:      StructMapper[Tagged](),
		ExpectedVal: Tagged{ID: 1, Name: "The Name", Email: "user@example.com"},
	})

	RunCustomStructMapperTest(t, "custom column separator", CustomStructMapperTest[Blog]{
		MapperTest: MapperTest[Blog]{
			row: &Row{
				columns: columnNames("id", "user,id", "user,name", "user,created_at"),
			},
			scanned: []any{100, 10, "The Name", now},
			ExpectedVal: Blog{
				ID: 100,
				User: UserWithTimestamps{
					User:       User{ID: 10, Name: "The Name"},
					Timestamps: &Timestamps{CreatedAt: now},
				},
			},
		},
		Options: []MappingSourceOption{WithColumnSeparator(",")},
	})

	RunCustomStructMapperTest(t, "custom name mapper", CustomStructMapperTest[Blog]{
		MapperTest: MapperTest[Blog]{
			row: &Row{
				columns: columnNames("ID", "USER.ID", "USER.NAME", "USER.CREATEDAT"),
			},
			scanned: []any{100, 10, "The Name", now},
			ExpectedVal: Blog{
				ID: 100,
				User: UserWithTimestamps{
					User:       User{ID: 10, Name: "The Name"},
					Timestamps: &Timestamps{CreatedAt: now},
				},
			},
		},
		Options: []MappingSourceOption{WithFieldNameMapper(strings.ToUpper)},
	})

	RunCustomStructMapperTest(t, "custom tag", CustomStructMapperTest[Tagged]{
		MapperTest: MapperTest[Tagged]{
			row: &Row{
				columns: columnNames("custom_id", "custom_name"),
			},
			scanned:     []any{1, "The Name"},
			ExpectedVal: Tagged{ID: 1, Name: "The Name"},
		},
		Options: []MappingSourceOption{WithStructTagKey("custom")},
	})

	RunMapperTest(t, "with prefix", MapperTest[User]{
		row: &Row{
			columns: columnNames("prefix--id", "prefix--name"),
		},
		scanned:     []any{1, "The Name"},
		Mapper:      StructMapper[User](WithStructTagPrefix("prefix--")),
		ExpectedVal: User{ID: 1, Name: "The Name"},
	})

	RunMapperTest(t, "with prefix and non-prefixed column", MapperTest[User]{
		row: &Row{
			columns: columnNames("id", "prefix--name"),
		},
		scanned:     []any{1, "The Name"},
		Mapper:      StructMapper[User](WithStructTagPrefix("prefix--")),
		ExpectedVal: User{ID: 0, Name: "The Name"},
	})

	RunMapperTest(t, "with type converter", MapperTest[User]{
		row: &Row{
			columns: columnNames("id", "name"),
		},
		scanned:     []any{wrapper{toPtr(1)}, wrapper{toPtr("The Name")}},
		Mapper:      StructMapper[User](WithTypeConverter(typeConverter{})),
		ExpectedVal: User{ID: 1, Name: "The Name"},
	})

	RunMapperTest(t, "with type converter ptr", MapperTest[*User]{
		row: &Row{
			columns: columnNames("id", "name"),
		},
		scanned:     []any{wrapper{toPtr(1)}, wrapper{toPtr("The Name")}},
		Mapper:      StructMapper[*User](WithTypeConverter(typeConverter{})),
		ExpectedVal: &User{ID: 1, Name: "The Name"},
	})

	RunMapperTest(t, "with type converter deep", MapperTest[PtrUser2]{
		row: &Row{
			columns: columnNames("id", "name", "created_at", "updated_at"),
		},
		scanned: []any{
			wrapper{toPtr(1)},
			wrapper{toPtr("The Name")},
			wrapper{&now},
			wrapper{toPtr(now.Add(time.Hour))},
		},
		Mapper: StructMapper[PtrUser2](WithTypeConverter(typeConverter{})),
		ExpectedVal: PtrUser2{
			ID: 1, Name: toPtr("The Name"),
			PtrTimestamps: &PtrTimestamps{CreatedAt: &now, UpdatedAt: toPtr(now.Add(time.Hour))},
		},
	})

	RunMapperTest(t, "with row validator pass", MapperTest[User]{
		row: &Row{
			columns: columnNames("id", "name"),
		},
		scanned: []any{1, "The Name"},
		Mapper: StructMapper[User](WithRowValidator(func(cols []string, vals []reflect.Value) bool {
			for i, c := range cols {
				if c == "id" {
					return vals[i].Elem().Int() == 1
				}
			}

			return false
		})),
		ExpectedVal: User{ID: 1, Name: "The Name"},
	})

	RunMapperTest(t, "with row validator fail", MapperTest[User]{
		row: &Row{
			columns: columnNames("id", "name"),
		},
		scanned: []any{1, "The Name"},
		Mapper: StructMapper[User](WithRowValidator(func(cols []string, vals []reflect.Value) bool {
			for i, c := range cols {
				if c == "id" {
					return vals[i].Elem().Int() == 0
				}
			}

			return false
		})),
		ExpectedVal: User{ID: 0, Name: ""},
	})

	RunMapperTest(t, "with mod", MapperTest[*User]{
		row: &Row{
			columns: columnNames("id", "name"),
		},
		scanned:     []any{2, "The Name"},
		Mapper:      CustomStructMapper[*User](defaultStructMapper, WithMapperMods(userMod)),
		ExpectedVal: &User{ID: 400, Name: "The Name modified"},
	})
}

func TestScannable(t *testing.T) {
	type scannable interface {
		Scan()
	}

	type BlogWithScannableUser struct {
		ID   int
		User ScannableUser
	}

	src, err := NewStructMapperSource(WithScannableTypes(
		(*scannable)(nil),
	))
	if err != nil {
		t.Fatalf("couldn't get mapper source: %v", err)
	}

	m, err := src.getMapping(reflect.TypeOf(BlogWithScannableUser{}))
	if err != nil {
		t.Fatalf("couldn't get mapping: %v", err)
	}

	var marked bool
	for _, info := range m {
		if info.name == "user" {
			marked = true
		}
	}

	if !marked {
		t.Fatal("did not mark user as scannable")
	}
}

func TestScannableErrors(t *testing.T) {
	cases := map[string]struct {
		typ any
		err error
	}{
		"nil": {
			typ: nil,
			err: fmt.Errorf("scannable type must be a pointer, got <nil>"),
		},
		"non-pointer": {
			typ: User{},
			err: fmt.Errorf("scannable type must be a pointer, got struct: scan.User"),
		},
		"non-interface": {
			typ: &User{},
			err: fmt.Errorf("scannable type must be a pointer to an interface, got struct: scan.User"),
		},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := NewStructMapperSource(WithScannableTypes(test.typ))
			if diff := diffErr(test.err, err); diff != "" {
				t.Fatalf("diff: %s", diff)
			}
		})
	}
}
