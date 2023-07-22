package libsql

import (
	"database/sql"
	"log"
	"os"
	"testing"
)

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
		t.Parallel()
		dir, err := os.MkdirTemp("", "libsql-*")
		if err != nil {
			log.Fatal(err)
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
	})
}

func TestOpenAndClose(t *testing.T) {
	runMemoryAndFileTests(t, func(t *testing.T, db *sql.DB) {
		// We don't have to do anything because runMemoryAndFileTests already opens and closes the database.
	})
}
