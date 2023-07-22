//go:build cgo
// +build cgo

package libsql

/*
#cgo CFLAGS: -I../c/include
#cgo LDFLAGS: -L../../target/debug
#cgo LDFLAGS: -lsql_experimental
#cgo LDFLAGS: -L../../../.libs
#cgo LDFLAGS: -lsqlite3
#cgo LDFLAGS: -lm
#include <libsql.h>
#include <stdlib.h>
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"unsafe"
)

func init() {
	sql.Register("libsql", &Driver{})
}

type database struct {
	nativePtr  C.libsql_database_t
	usageCount int
}

type Driver struct {
	mu  sync.Mutex
	dbs map[string]*database
}

func libsqlOpen(dataSourceName string) (C.libsql_database_t, error) {
	connectionString := C.CString(dataSourceName)
	defer C.free(unsafe.Pointer(connectionString))

	db := C.libsql_open_ext(connectionString)
	if db == nil {
		return nil, fmt.Errorf("failed to open database")
	}
	return db, nil
}

func libsqlConnect(db C.libsql_database_t) (C.libsql_connection_t, error) {
	conn := C.libsql_connect(db)
	if conn == nil {
		return nil, fmt.Errorf("failed to connect to database")
	}
	return conn, nil
}

func (d *Driver) getConnection(dataSourceName string) (C.libsql_connection_t, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.dbs == nil {
		d.dbs = make(map[string]*database)
	}
	var db *database
	if db, ok := d.dbs[dataSourceName]; !ok {
		nativePtr, err := libsqlOpen(dataSourceName)
		if err != nil {
			return nil, err
		}
		db = &database{nativePtr, 0}
		d.dbs[dataSourceName] = db
	}
	connNativePtr, err := libsqlConnect(db.nativePtr)
	if err != nil {
		if db.usageCount == 0 {
			C.libsql_close(db.nativePtr)
			delete(d.dbs, dataSourceName)
		}
		return nil, err
	}
	db.usageCount++
	return connNativePtr, nil
}

func (d *Driver) Open(dataSourceName string) (driver.Conn, error) {
	nativePtr, err := d.getConnection(dataSourceName)
	if err != nil {
		return nil, err
	}
	return &conn{d, nativePtr, dataSourceName}, nil
}

func (d *Driver) CloseConnection(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	db, ok := d.dbs[name]
	if !ok {
		return fmt.Errorf("database %s not found", name)
	}
	db.usageCount--
	if db.usageCount == 0 {
		C.libsql_close(db.nativePtr)
		delete(d.dbs, name)
	}
	return nil
}

type conn struct {
	driver         *Driver
	nativePtr      C.libsql_connection_t
	dataSourceName string
}

func (c *conn) Close() error {
	return c.driver.CloseConnection(c.dataSourceName)
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare() is not implemented")
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("begin() is not implemented")
}
