//go:build cgo
// +build cgo

package libsql

/*
#cgo CFLAGS: -I../c/include
#cgo LDFLAGS: ../../../target/debug/libsql_experimental.a
#cgo LDFLAGS: ../../../../.libs/libsqlite3.a
#cgo LDFLAGS: -lm
#include <libsql.h>
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
)

func init() {
	sql.Register("libsql", Driver{})
}

type Driver struct{}

func (d Driver) Open(dataSourceName string) (driver.Conn, error) {
	connectionString := C.CString(dataSourceName)
	// TODO: defer C.free(unsafe.Pointer(connectionString))

	_ = C.libsql_open_ext(connectionString)

	return &conn{}, nil
}

type conn struct {
}

func (c *conn) Close() error {
	return nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("Prepare() is not implemented")
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("Begin() is not implemented")
}
