package libsql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"gotest.tools/assert"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

func executeSql(t *testing.T, primaryUrl, sql string) {
	type statement struct {
		Query string `json:"q"`
	}
	type postBody struct {
		Statements []statement `json:"statements"`
	}

	type resultSet struct {
		Columns []string `json:"columns"`
	}

	type httpErrObject struct {
		Message string `json:"message"`
	}

	type httpResults struct {
		Results *resultSet     `json:"results"`
		Error   *httpErrObject `json:"error"`
	}

	type httpResultsAlternative struct {
		Results *resultSet `json:"results"`
		Error   string     `json:"error"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rawReq := postBody{}

	rawReq.Statements = append(rawReq.Statements, statement{Query: sql})

	body, err := json.Marshal(rawReq)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", primaryUrl+"", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatal("unexpected status code: ", resp.StatusCode)
	}
	var results []httpResults

	err = json.Unmarshal(body, &results)
	if err != nil {
		var alternativeResults []httpResultsAlternative
		errArray := json.Unmarshal(body, &alternativeResults)
		if errArray != nil {
			t.Fatal("failed to unmarshal response: ", err, errArray)
		}
		if alternativeResults[0].Error != "" {
			t.Fatal(errors.New(alternativeResults[0].Error))
		}
	} else {
		if results[0].Error != nil {
			t.Fatal(errors.New(results[0].Error.Message))
		}
		if results[0].Results == nil {
			t.Fatal(errors.New("no results"))
		}
	}
}

func insertRow(t *testing.T, dbUrl, tableName string, id int) {
	executeSql(t, dbUrl, fmt.Sprintf("INSERT INTO %s (id, name, gpa, cv) VALUES (%d, '%d', %d.5, randomblob(10));", tableName, id, id, id))
}

func insertRows(t *testing.T, dbUrl, tableName string, start, count int) {
	for i := 0; i < count; i++ {
		insertRow(t, dbUrl, tableName, start+i)
	}
}

func createTable(t *testing.T, dbPath string) string {
	tableName := fmt.Sprintf("test_%d", time.Now().UnixNano())
	executeSql(t, dbPath, fmt.Sprintf("CREATE TABLE %s (id INTEGER, name TEXT, gpa REAL, cv BLOB);", tableName))
	return tableName
}

func removeTable(t *testing.T, dbPath, tableName string) {
	executeSql(t, dbPath, fmt.Sprintf("DROP TABLE %s;", tableName))
}

func testSync(t *testing.T, connect func(dbPath, primaryUrl string) *Connector, sync func(connector *Connector)) {
	primaryUrl := os.Getenv("LIBSQL_PRIMARY_URL")
	if primaryUrl == "" {
		t.Skip("LIBSQL_PRIMARY_URL is not set")
		return
	}
	tableName := createTable(t, primaryUrl)
	defer removeTable(t, primaryUrl, tableName)

	initialRowsCount := 5
	insertRows(t, primaryUrl, tableName, 0, initialRowsCount)
	dir, err := os.MkdirTemp("", "libsql-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	connector := connect(dir+"/test.db", primaryUrl)
	db := sql.OpenDB(connector)
	defer db.Close()

	iterCount := 2
	for iter := 0; iter < iterCount; iter++ {
		func() {
			rows, err := db.QueryContext(context.Background(), "SELECT NULL, id, name, gpa, cv FROM "+tableName)
			if err != nil {
				t.Fatal(err)
			}
			columns, err := rows.Columns()
			if err != nil {
				t.Fatal(err)
			}
			assert.DeepEqual(t, columns, []string{"NULL", "id", "name", "gpa", "cv"})
			types, err := rows.ColumnTypes()
			if err != nil {
				t.Fatal(err)
			}
			if len(types) != 5 {
				t.Fatal("types should be 5")
			}
			defer rows.Close()
			idx := 0
			for rows.Next() {
				if idx > initialRowsCount+iter {
					t.Fatal("idx should be <= ", initialRowsCount+iter)
				}
				var null any
				var id int
				var name string
				var gpa float64
				var cv []byte
				if err := rows.Scan(&null, &id, &name, &gpa, &cv); err != nil {
					t.Fatal(err)
				}
				if null != nil {
					t.Fatal("null should be nil")
				}
				if id != int(idx) {
					t.Fatal("id should be ", idx, " got ", id)
				}
				if name != fmt.Sprint(idx) {
					t.Fatal("name should be", idx)
				}
				if gpa != float64(idx)+0.5 {
					t.Fatal("gpa should be", float64(idx)+0.5)
				}
				if len(cv) != 10 {
					t.Fatal("cv should be 10 bytes")
				}
				idx++
			}
			if idx != initialRowsCount+iter {
				t.Fatal("idx should be ", initialRowsCount+iter, " got ", idx)
			}
		}()
		if iter+1 != iterCount {
			insertRow(t, primaryUrl, tableName, initialRowsCount+iter)
			sync(connector)
		}
	}
}

func TestAutoSync(t *testing.T) {
	syncInterval := 1 * time.Second
	testSync(t, func(dbPath, primaryUrl string) *Connector {
		connector, err := NewEmbeddedReplicaConnectorWithAutoSync(dbPath, primaryUrl, syncInterval)
		if err != nil {
			t.Fatal(err)
		}
		return connector
	}, func(_ *Connector) {
		time.Sleep(2 * syncInterval)
	})
}

func TestSync(t *testing.T) {
	testSync(t, func(dbPath, primaryUrl string) *Connector {
		connector, err := NewEmbeddedReplicaConnector(dbPath, primaryUrl)
		if err != nil {
			t.Fatal(err)
		}
		return connector
	}, func(c *Connector) {
		if err := c.Sync(); err != nil {
			t.Fatal(err)
		}
	})
}

func runFileTest(t *testing.T, test func(*testing.T, *sql.DB)) {
	t.Parallel()
	dir, err := os.MkdirTemp("", "libsql-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	db, err := sql.Open("libsql", dir+"/test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	test(t, db)
}

func runMemoryAndFileTests(t *testing.T, test func(*testing.T, *sql.DB)) {
	t.Parallel()
	t.Run("Memory", func(t *testing.T) {
		t.Parallel()
		db, err := sql.Open("libsql", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}()
		test(t, db)
	})
	t.Run("File", func(t *testing.T) {
		runFileTest(t, test)
	})
}

func TestErrorNonUtf8URL(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("libsql", "a\xc5z")
	if err == nil {
		defer func() {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}()
		t.Fatal("expected error")
	}
	if err.Error() != "failed to open database a\xc5z\nerror code = 1: Wrong URL: invalid utf-8 sequence of 1 bytes from index 1" {
		t.Fatal("unexpected error:", err)
	}
}

func TestErrorWrongURL(t *testing.T) {
	t.Skip("Does not work with v2")
	t.Parallel()
	db, err := sql.Open("libsql", "http://example.com/test")
	if err == nil {
		defer func() {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}()
		t.Fatal("expected error")
	}
	if err.Error() != "failed to open database http://example.com/test\nerror code = 1: Error opening URL http://example.com/test: Failed to connect to database: `Unable to open remote database http://example.com/test with Database::open()`" {
		t.Fatal("unexpected error:", err)
	}
}

func TestErrorCanNotConnect(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("libsql", "/root/test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	conn, err := db.Conn(context.Background())
	if err == nil {
		defer func() {
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
		}()
		t.Fatal("expected error")
	}
	if err.Error() != "failed to connect to database\nerror code = 1: Unable to connect: Failed to connect to database: `/root/test.db`" {
		t.Fatal("unexpected error:", err)
	}
}

func TestExec(t *testing.T) {
	runMemoryAndFileTests(t, func(t *testing.T, db *sql.DB) {
		if _, err := db.ExecContext(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT)"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestExecWithQuery(t *testing.T) {
	runMemoryAndFileTests(t, func(t *testing.T, db *sql.DB) {
		if _, err := db.ExecContext(context.Background(), "SELECT 1"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestErrorExec(t *testing.T) {
	runMemoryAndFileTests(t, func(t *testing.T, db *sql.DB) {
		_, err := db.ExecContext(context.Background(), "CREATE TABLES test (id INTEGER, name TEXT)")
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != "failed to execute query CREATE TABLES test (id INTEGER, name TEXT)\nerror code = 1: Error executing statement: Failed to prepare statement `CREATE TABLES test (id INTEGER, name TEXT)`: `near \"TABLES\": syntax error`" {
			t.Fatal("unexpected error:", err)
		}
	})
}

func TestQuery(t *testing.T) {
	runMemoryAndFileTests(t, func(t *testing.T, db *sql.DB) {
		if _, err := db.ExecContext(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT, gpa REAL, cv BLOB)"); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 10; i++ {
			if _, err := db.ExecContext(context.Background(), "INSERT INTO test VALUES("+fmt.Sprint(i)+", '"+fmt.Sprint(i)+"', "+fmt.Sprint(i)+".5, randomblob(10))"); err != nil {
				t.Fatal(err)
			}
		}
		rows, err := db.QueryContext(context.Background(), "SELECT NULL, id, name, gpa, cv FROM test")
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		assert.DeepEqual(t, columns, []string{"NULL", "id", "name", "gpa", "cv"})
		types, err := rows.ColumnTypes()
		if err != nil {
			t.Fatal(err)
		}
		if len(types) != 5 {
			t.Fatal("types should be 5")
		}
		defer rows.Close()
		idx := 0
		for rows.Next() {
			var null any
			var id int
			var name string
			var gpa float64
			var cv []byte
			if err := rows.Scan(&null, &id, &name, &gpa, &cv); err != nil {
				t.Fatal(err)
			}
			if null != nil {
				t.Fatal("null should be nil")
			}
			if id != int(idx) {
				t.Fatal("id should be", idx)
			}
			if name != fmt.Sprint(idx) {
				t.Fatal("name should be", idx)
			}
			if gpa != float64(idx)+0.5 {
				t.Fatal("gpa should be", float64(idx)+0.5)
			}
			if len(cv) != 10 {
				t.Fatal("cv should be 10 bytes")
			}
			idx++
		}
	})
}

func TestErrorQuery(t *testing.T) {
	runMemoryAndFileTests(t, func(t *testing.T, db *sql.DB) {
		rows, err := db.QueryContext(context.Background(), "SELECT NULL, id, name, gpa, cv FROM test")
		if rows != nil {
			rows.Close()
		}
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != "failed to execute query SELECT NULL, id, name, gpa, cv FROM test\nerror code = 1: Error executing statement: Failed to prepare statement `SELECT NULL, id, name, gpa, cv FROM test`: `no such table: test`" {
			t.Fatal("unexpected error:", err)
		}
	})
}

func TestQueryWithEmptyResult(t *testing.T) {
	runMemoryAndFileTests(t, func(t *testing.T, db *sql.DB) {
		if _, err := db.ExecContext(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT, gpa REAL, cv BLOB)"); err != nil {
			t.Fatal(err)
		}
		rows, err := db.QueryContext(context.Background(), "SELECT NULL, id, name, gpa, cv FROM test")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		assert.DeepEqual(t, columns, []string{"NULL", "id", "name", "gpa", "cv"})
		types, err := rows.ColumnTypes()
		if err != nil {
			t.Fatal(err)
		}
		if len(types) != 5 {
			t.Fatal("types should be 5")
		}
		for rows.Next() {
			t.Fatal("there should be no rows")
		}
	})
}

func TestErrorRowsNext(t *testing.T) {
	runFileTest(t, func(t *testing.T, db *sql.DB) {
		db.Exec("PRAGMA journal_mode=DELETE")
		if _, err := db.ExecContext(context.Background(), "CREATE TABLE test (id INTEGER)"); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 10; i++ {
			if _, err := db.ExecContext(context.Background(), "INSERT INTO test VALUES("+fmt.Sprint(i)+")"); err != nil {
				t.Fatal(err)
			}
		}
		c1, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer c1.Close()
		c1.ExecContext(context.Background(), "PRAGMA journal_mode=DELETE")
		c2, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer c2.Close()
		c2.ExecContext(context.Background(), "PRAGMA journal_mode=DELETE")
		_, err = c1.ExecContext(context.Background(), "BEGIN EXCLUSIVE TRANSACTION")
		if err != nil {
			t.Fatal(err)
		}
		rows, err := c2.QueryContext(context.Background(), "SELECT id FROM test")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		if rows.Next() {
			t.Fatal("there should be no rows")
		}
		err = rows.Err()
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != "failed to get next row\nerror code = 1: Error fetching next row: Failed to fetch row: `database is locked`" {
			t.Fatal("unexpected error:", err)
		}
	})
}
