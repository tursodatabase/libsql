package main

import (
	"database/sql"
	"log"

	_ "github.com/libsql/go-libsql"
)

func main() {
	db, err := sql.Open("libsql", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.QueryRow("SELECT 1")
}
