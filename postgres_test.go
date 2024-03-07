package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/randlabs/go-postgres"
)

// -----------------------------------------------------------------------------

const (
	varCharText  = "varchar-sample"
	charText     = "char-sample"
	veryLongText = "this is a big text"
)

type TestRowDef struct {
	id   int
	num  uint64
	sm   int16
	bi   int64
	bi2  uint64
	dbl  float64
	va   string
	chr  string
	txt  string
	blob []byte
	ts   time.Time
	dt   time.Time
	tim  time.Time
	b    bool
	js   string
}

type TestNullableRowDef struct {
	id   int
	num  *uint64
	sm   *int16
	bi   *int64
	bi2  *uint64
	dbl  *float64
	va   *string
	chr  *string
	txt  *string
	blob *[]byte
	ts   *time.Time
	dt   *time.Time
	tim  *time.Time
	b    *bool
	js   *string
}

type TestJSON struct {
	Id   int    `json:"id"`
	Text string `json:"text"`
}

var (
	pgUrl          string
	pgHost         string
	pgPort         uint
	pgUsername     string
	pgPassword     string
	pgDatabaseName string
)

var (
	testJSON      TestJSON
	testBLOB      []byte
	testJSONBytes []byte
)

// -----------------------------------------------------------------------------

func init() {
	flag.StringVar(&pgUrl, "url", "", "Specifies the Postgres URL.")
	flag.StringVar(&pgHost, "host", "127.0.0.1", "Specifies the Postgres server host. (Defaults to '127.0.0.1')")
	flag.UintVar(&pgPort, "port", 5432, "Specifies the Postgres server port. (Defaults to 5432)")
	flag.StringVar(&pgUsername, "user", "postgres", "Specifies the user name. (Defaults to 'postgres')")
	flag.StringVar(&pgPassword, "password", "", "Specifies the user password.")
	flag.StringVar(&pgDatabaseName, "db", "", "Specifies the database name.")

	testJSON = TestJSON{
		Id:   1,
		Text: "demo",
	}

	testBLOB = make([]byte, 1024)
	_, _ = rand.Read(testBLOB)

	testJSONBytes, _ = json.Marshal(testJSON)
}

// -----------------------------------------------------------------------------

func TestPostgres(t *testing.T) {
	var db *postgres.Database
	var err error

	// Parse and check command-line parameters
	flag.Parse()
	checkSettings(t)

	ctx := context.Background()

	// Create database driver
	if len(pgUrl) > 0 {
		db, err = postgres.NewFromURL(ctx, pgUrl)
	} else {
		db, err = postgres.New(ctx, postgres.Options{
			Host:     pgHost,
			Port:     uint16(pgPort),
			User:     pgUsername,
			Password: pgPassword,
			Name:     pgDatabaseName,
		})
	}
	if err != nil {
		t.Fatalf("%v", err.Error())
	}
	defer db.Close()

	t.Log("Creating test table")
	err = createTestTable(ctx, db)
	if err != nil {
		t.Fatalf("%v", err.Error())
	}

	t.Log("Inserting test data")
	err = insertTestData(ctx, db)
	if err != nil {
		t.Fatalf("%v", err.Error())
	}

	t.Log("Reading test data")
	err = readTestData(ctx, db)
	if err != nil {
		t.Fatalf("%v", err.Error())
	}

	t.Log("Reading test data (multi-row)")
	err = readMultiTestData(ctx, db)
	if err != nil {
		t.Fatalf("%v", err.Error())
	}
}

// -----------------------------------------------------------------------------

func checkSettings(t *testing.T) {
	if len(pgHost) == 0 {
		t.Fatalf("Server host not specified")
	}
	if pgPort > 65535 {
		t.Fatalf("Server port not specified or invalid")
	}
	if len(pgUsername) == 0 {
		t.Fatalf("User name to access database server not specified")
	}
	if len(pgPassword) == 0 {
		t.Fatalf("User password to access database server not specified")
	}
	if len(pgDatabaseName) == 0 {
		t.Fatalf("Database name not specified")
	}
}

func createTestTable(ctx context.Context, db *postgres.Database) error {
	// Destroy old test table if exists
	_, err := db.Exec(ctx, `DROP TABLE IF EXISTS go_postgres_test_table CASCADE`)
	if err != nil {
		return fmt.Errorf("Unable to drop tables [err=%v]", err.Error())
	}

	// Create the test table
	_, err = db.Exec(ctx, `CREATE TABLE go_postgres_test_table (
		id   INT NOT NULL,
		num  NUMERIC(24, 0) NULL,
		sm   SMALLINT NULL,
		bi   BIGINT NULL,
		bi2  BIGINT NULL,
		dbl  DOUBLE PRECISION NULL,
		va   VARCHAR(32) NULL,
		chr  CHAR(32) NULL,
		txt  TEXT NULL,
		blob BYTEA NULL,
		ts   TIMESTAMP NULL,
		dt   DATE NULL,
		tim  TIME NULL,
		b    BOOLEAN NULL,
		js   JSONB NULL,

		PRIMARY KEY (id)
	)`)
	if err != nil {
		return fmt.Errorf("Unable to create test table [err=%v]", err.Error())
	}

	// Done
	return nil
}

func insertTestData(ctx context.Context, db *postgres.Database) error {
	return db.WithinTx(ctx, func(ctx context.Context, tx postgres.Tx) error {
		for idx := 1; idx <= 2; idx++ {
			rd := genTestRowDef(idx, true)
			err := insertTestRowDef(ctx, tx, rd)
			if err != nil {
				return fmt.Errorf("Unable to insert test data [id=%v/err=%v]", rd.id, err.Error())
			}

			nrd := genTestNullableRowDef(idx, true)
			err = insertTestNullableRowDef(ctx, tx, nrd)
			if err != nil {
				return fmt.Errorf("Unable to insert test data [id=%v/err=%v]", nrd.id, err.Error())
			}
		}
		// Done
		return nil
	})
}

func readTestData(ctx context.Context, db *postgres.Database) error {
	for idx := 1; idx <= 2; idx++ {
		compareRd := genTestRowDef(idx, false)
		rd, err := readTestRowDef(ctx, db, compareRd.id)
		if err != nil {
			return fmt.Errorf("Unable to verify test data [id=%v/err=%v]", compareRd.id, err.Error())
		}
		// Do deep comparison
		if !reflect.DeepEqual(compareRd, rd) {
			return errors.New("data mismatch")
		}

		compareNrd := genTestNullableRowDef(idx, false)
		nrd, err := readTestNullableRowDef(ctx, db, compareNrd.id)
		if err != nil {
			return fmt.Errorf("Unable to verify test data [id=%v/err=%v]", compareNrd.id, err.Error())
		}

		// Do deep comparison
		if !reflect.DeepEqual(compareNrd, nrd) {
			return fmt.Errorf("Data mismatch while comparing test data [id=%v]", compareNrd.id)
		}
	}

	// Done
	return nil
}

func readMultiTestData(ctx context.Context, db *postgres.Database) error {
	compareRd := make([]TestRowDef, 0)
	for idx := 1; idx <= 2; idx++ {
		compareRd = append(compareRd, genTestRowDef(idx, false))
	}
	rd, err := readMultiTestRowDef(ctx, db, compareRd)
	if err != nil {
		return fmt.Errorf("Unable to verify test data [err=%v]", err.Error())
	}

	// Do deep comparison
	if len(compareRd) != len(rd) {
		return fmt.Errorf("Data mismatch while comparing test data [len1=%d/len2=%d]", len(compareRd), len(rd))
	}

	for idx := 0; idx < len(rd); idx++ {
		if !reflect.DeepEqual(compareRd[idx], rd[idx]) {
			return fmt.Errorf("Data mismatch while comparing test data [id=%v]", compareRd[idx].id)
		}
	}

	// Done
	return nil
}

func genTestRowDef(index int, write bool) TestRowDef {
	var r TestRowDef

	switch index {
	case 1:
		r = TestRowDef{
			id:   1,
			num:  math.MaxUint64,
			sm:   math.MaxInt16,
			bi:   math.MaxInt64,
			bi2:  math.MaxInt64, // BIGINT is signed so sending an uint64 that does not overflow
			dbl:  math.MaxFloat64,
			va:   varCharText,
			chr:  charText,
			txt:  veryLongText,
			blob: testBLOB,
			ts:   time.Date(2022, 12, 31, 23, 59, 59, 0, time.UTC),
			dt:   time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC),
			tim:  time.Date(2000, 1, 1, 23, 59, 59, 0, time.UTC),
			b:    false,
			js:   string(testJSONBytes),
		}
		if !write {
			r.chr = charText + strings.Repeat(" ", 32-len(charText))
		}

	case 2:
		r = TestRowDef{
			id:   2,
			num:  math.MaxUint64,
			sm:   math.MinInt16,
			bi:   math.MinInt64,
			bi2:  math.MaxInt64, // BIGINT is signed so sending an uint64 that does not overflow
			dbl:  math.SmallestNonzeroFloat64,
			va:   varCharText,
			chr:  charText,
			txt:  veryLongText,
			blob: testBLOB,
			ts:   time.Date(2022, 12, 31, 23, 59, 59, 0, time.UTC),
			dt:   time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC),
			tim:  time.Date(2000, 1, 1, 23, 59, 59, 0, time.UTC),
			b:    false,
			js:   string(testJSONBytes),
		}
		if !write {
			r.chr = charText + strings.Repeat(" ", 32-len(charText))
		}

	default:
		panic("unexpected")
	}
	return r
}

func genTestNullableRowDef(index int, write bool) TestNullableRowDef {
	var r TestNullableRowDef

	switch index {
	case 1:
		r = TestNullableRowDef{
			id:   101,
			num:  addressOf[uint64](math.MaxUint64),
			sm:   addressOf[int16](math.MaxInt16),
			bi:   addressOf[int64](math.MaxInt64),
			bi2:  addressOf[uint64](math.MaxInt64), // BIGINT is signed so sending an uint64 that does not overflow
			dbl:  addressOf[float64](math.MaxFloat64),
			va:   addressOf[string](varCharText),
			chr:  addressOf[string](charText),
			txt:  nil,
			blob: nil,
			ts:   nil,
			dt:   nil,
			tim:  nil,
			b:    nil,
			js:   nil,
		}
		if !write {
			r.chr = addressOf[string](charText + strings.Repeat(" ", 32-len(charText)))
		}

	case 2:
		r = TestNullableRowDef{
			id:   102,
			num:  nil,
			sm:   nil,
			bi:   nil,
			bi2:  nil,
			dbl:  nil,
			va:   nil,
			chr:  nil,
			txt:  addressOf[string](veryLongText),
			blob: addressOf[[]byte](testBLOB),
			ts:   addressOf[time.Time](time.Date(2022, 12, 31, 23, 59, 59, 0, time.UTC)),
			dt:   addressOf[time.Time](time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC)),
			tim:  addressOf[time.Time](time.Date(2000, 1, 1, 23, 59, 59, 0, time.UTC)),
			b:    addressOf[bool](false),
			js:   addressOf[string](string(testJSONBytes)),
		}

	default:
		panic("unexpected")
	}
	return r
}

func insertTestRowDef(ctx context.Context, tx postgres.Tx, rd TestRowDef) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO go_postgres_test_table (
			id, num, sm, bi, bi2, dbl, va, chr, txt, blob, ts, dt, tim, b, js
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
		)
	`,
		rd.id, rd.num, rd.sm, rd.bi, rd.bi2, rd.dbl, rd.va, rd.chr, rd.txt, rd.blob, rd.ts, rd.dt, rd.tim, rd.b, rd.js,
	)
	return err
}

func readTestRowDef(ctx context.Context, db *postgres.Database, id int) (TestRowDef, error) {
	rd := TestRowDef{}
	err := db.QueryRow(ctx, `
		SELECT
			id, num, sm, bi, bi2, dbl, va, chr, txt, blob, ts, dt, tim, b, js
		FROM
			go_postgres_test_table
		WHERE
			id = $1
	`, id).Scan(
		&rd.id, &rd.num, &rd.sm, &rd.bi, &rd.bi2, &rd.dbl, &rd.va, &rd.chr, &rd.txt, &rd.blob, &rd.ts, &rd.dt, &rd.tim,
		&rd.b, &rd.js,
	)
	if err != nil {
		return TestRowDef{}, err
	}

	// JSON data returned by Postgres can contain spaces and other encoding so re-encode the returned string
	// for comparison
	rd.js, err = jsonReEncode(rd.js)
	if err != nil {
		return TestRowDef{}, err
	}

	// Done
	return rd, nil
}

func readMultiTestRowDef(ctx context.Context, db *postgres.Database, compareRd []TestRowDef) ([]TestRowDef, error) {
	// Populate ids
	ids := make([]int, len(compareRd))
	for idx := 0; idx < len(compareRd); idx++ {
		ids[idx] = compareRd[idx].id
	}

	rd := make([]TestRowDef, 0)
	err := db.QueryRows(ctx, `
		SELECT
			id, num, sm, bi, bi2, dbl, va, chr, txt, blob, ts, dt, tim, b, js
		FROM
			go_postgres_test_table
		WHERE
			id = ANY($1)
	`, ids).Do(func(ctx context.Context, row postgres.Row) (bool, error) {
		item := TestRowDef{}
		err := row.Scan(&item.id, &item.num, &item.sm, &item.bi, &item.bi2, &item.dbl, &item.va, &item.chr, &item.txt,
			&item.blob, &item.ts, &item.dt, &item.tim, &item.b, &item.js)
		if err == nil {
			rd = append(rd, item)
		}
		return true, err
	})
	if err != nil {
		return nil, err
	}

	// JSON data returned by Postgres can contain spaces and other encoding so re-encode the returned string
	// for comparison
	for idx := range rd {
		rd[idx].js, err = jsonReEncode(rd[idx].js)
		if err != nil {
			return nil, err
		}
	}

	// Done
	return rd, nil
}

func insertTestNullableRowDef(ctx context.Context, tx postgres.Tx, nrd TestNullableRowDef) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO go_postgres_test_table (
			id, num, sm, bi, bi2, dbl, va, chr, txt, blob, ts, dt, tim, b, js
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
		)
	`,
		nrd.id, nrd.num, nrd.sm, nrd.bi, nrd.bi2, nrd.dbl, nrd.va, nrd.chr, nrd.txt, nrd.blob, nrd.ts, nrd.dt, nrd.tim,
		nrd.b, nrd.js,
	)
	return err
}

func readTestNullableRowDef(ctx context.Context, db *postgres.Database, id int) (TestNullableRowDef, error) {
	nrd := TestNullableRowDef{}
	err := db.QueryRow(ctx, `
		SELECT
			id, num, sm, bi, bi2, dbl, va, chr, txt, blob, ts, dt, tim, b, js::text
		FROM
			go_postgres_test_table
		WHERE
			id = $1
	`, id).Scan(
		&nrd.id, &nrd.num, &nrd.sm, &nrd.bi, &nrd.bi2, &nrd.dbl, &nrd.va, &nrd.chr, &nrd.txt, &nrd.blob, &nrd.ts,
		&nrd.dt, &nrd.tim, &nrd.b, &nrd.js,
	)
	if err != nil {
		return TestNullableRowDef{}, err
	}

	// JSON data returned by Postgres can contain spaces and other encoding so re-encode the returned string
	// for comparison
	if nrd.js != nil {
		var js string

		js, err = jsonReEncode(*nrd.js)
		if err != nil {
			return TestNullableRowDef{}, err
		}
		nrd.js = &js
	}

	// Done
	return nrd, nil
}

func addressOf[T any](x T) *T {
	return &x
}

func jsonReEncode(src string) (string, error) {
	var v interface{}

	err := json.Unmarshal([]byte(src), &v)
	if err == nil {
		var reencoded []byte

		reencoded, err = json.Marshal(v)
		if err == nil {
			return string(reencoded), nil
		}
	}
	return "", err
}
