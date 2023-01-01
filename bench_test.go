package scan

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	_ "github.com/stephenafamo/fakedb"
)

// Benchmark Command: go test -v -run=XXX -cpu 1,2,4 -benchmem -bench=. -memprofile mem.prof

var (
	db       *sql.DB
	dataSize = 100
)

func TestMain(m *testing.M) {
	var err error

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	db, err = sql.Open("test", "bench")
	if err != nil {
		panic(fmt.Errorf("Error opening testdb %w", err))
	}
	defer db.Close()

	err = prepareData(ctx)
	if err != nil {
		panic(err)
	}

	exitVal := m.Run()

	os.Exit(exitVal)
}

func BenchmarkScanAll(b *testing.B) {
	b.StopTimer()
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		rows, err := db.Query("SELECT|user||")
		if err != nil {
			panic(err)
		}
		b.StartTimer()
		if _, err := AllFromRows(ctx, StructMapper[Userss](), rows); err != nil {
			panic(err)
		}
		rows.Close()
	}
}

func BenchmarkScanOne(b *testing.B) {
	b.StopTimer()
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		rows, err := db.Query("SELECT|user||")
		if err != nil {
			panic(err)
		}
		b.StartTimer()
		if _, err := OneFromRows(ctx, StructMapper[Userss](), rows); err != nil {
			panic(err)
		}
		rows.Close()
	}
}

func prepareData(ctx context.Context) error {
	create := "CREATE|user|id=int64,username=string,password=string"
	create += ",email=string,mobile_phone=string,company=string,avatar_url=string"
	create += ",role=int16,last_online_at=int64,create_at=datetime,update_at=datetime"

	if _, err := db.ExecContext(ctx, create); err != nil {
		return err
	}

	for i := 0; i < dataSize; i++ {
		userName := fmt.Sprintf("user%d", i+1)
		password := fmt.Sprintf("password%d", i+1)
		email := fmt.Sprintf("user%d@sqlscan.com", i+1)
		mobilePhone := fmt.Sprintf("%d", 10000*(i+1))
		company := fmt.Sprintf("company%d", i+1)
		avatarURL := fmt.Sprintf("http://sqlscan.com/avatar/%d", i+1)
		role := i % 3
		lastOnlineAt := time.Now().Unix() + int64(i)
		createAt := time.Now().UTC()
		updateAt := time.Now().UTC()
		_, err := db.Exec(`INSERT|user|id=?,username=?,password=?,email=?,mobile_phone=?,company=?,avatar_url=?,role=?,last_online_at=?,create_at=?,update_at=?`,
			i, userName, password, email, mobilePhone, company, avatarURL, role, lastOnlineAt, createAt, updateAt)
		if err != nil {
			return err
		}
	}

	return nil
}

type Userss struct {
	ID           int       `db:"id"`
	UserName     string    `db:"username"`
	Password     string    `db:"password"`
	Email        string    `db:"email"`
	MobilePhone  string    `db:"mobile_phone"`
	Company      string    `db:"company"`
	AvatarURL    string    `db:"avatar_url"`
	Role         int       `db:"role"`
	LastOnlineAt int64     `db:"last_online_at"`
	CreateAt     time.Time `db:"create_at"`
	UpdateAt     time.Time `db:"update_at"`
}

func TestPrepare(t *testing.T) {
	cnt := 0
	rows, err := db.Query("SELECT|user||")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		cnt++
	}
	if cnt != dataSize {
		t.Error("wrong cnt")
	}
}
