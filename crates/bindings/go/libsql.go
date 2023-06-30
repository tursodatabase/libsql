package libsql

/*
#cgo CFLAGS: -I../c/include
#cgo LDFLAGS: -L../../target/debug -lsql_experimental
#include <libsql.h>
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("libsql", Driver{})
}

type Driver struct{}

func (d Driver) Open(dataSourceName string) (driver.Conn, error) {
	connectionString := C.CString(dataSourceName)
	// TODO: defer C.free(unsafe.Pointer(connectionString))

	_ = C.libsql_open_ext(connectionString)

	return nil, nil
}
