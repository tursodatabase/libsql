package libsql

import (
	"context"
	"database/sql"
	"fmt"
	"gotest.tools/assert"
	"os"
	"testing"
)

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
	if err.Error() != "failed to open database a\xc5z\nerror code = 1: Wrong URL: invalid utf-8 sequence of 1 bytes from index 1" {
		t.Fatal("unexpected error:", err)
	}
}

func TestErrorWrongURL(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("libsql", "http://example.com/test")
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
		if columns, err := rows.Columns(); len(columns) > 0 || err != nil {
			t.Fatal("columns should be nil")
		}
		if columnTypes, err := rows.ColumnTypes(); len(columnTypes) > 0 || err != nil {
			t.Fatal("column types should be nil")
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
