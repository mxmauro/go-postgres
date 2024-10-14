package postgres_test

import (
	"crypto/rand"
	"encoding/json"
	"flag"
	"testing"
)

// -----------------------------------------------------------------------------

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

func addressOf[T any](x T) *T {
	return &x
}

func jsonReEncode(src string) (string, error) {
	var v interface{}

	err := json.Unmarshal([]byte(src), &v)
	if err == nil {
		var reEncoded []byte

		reEncoded, err = json.Marshal(v)
		if err == nil {
			return string(reEncoded), nil
		}
	}
	return "", err
}
