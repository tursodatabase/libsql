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
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
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

func libsqlError(message string, statusCode C.int, errMsg *C.char) error {
	code := int(statusCode)
	if errMsg != nil {
		msg := C.GoString(errMsg)
		C.libsql_free_string(errMsg)
		return fmt.Errorf("%s\nerror code = %d: %v", message, code, msg)
	} else {
		return fmt.Errorf("%s\nerror code = %d", message, code)
	}
}

func libsqlOpen(dataSourceName string) (C.libsql_database_t, error) {
	connectionString := C.CString(dataSourceName)
	defer C.free(unsafe.Pointer(connectionString))

	var db C.libsql_database_t
	var errMsg *C.char
	statusCode := C.libsql_open_ext(connectionString, &db, &errMsg)
	if statusCode != 0 {
		return nil, libsqlError(fmt.Sprint("failed to open database ", dataSourceName), statusCode, errMsg)
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
	var ok bool
	if db, ok = d.dbs[dataSourceName]; !ok {
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

func (c *conn) execute(query string) C.libsql_rows_t {
	queryCString := C.CString(query)
	defer C.free(unsafe.Pointer(queryCString))

	return C.libsql_execute(c.nativePtr, queryCString)
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	rows := c.execute(query)
	if rows != nil {
		C.libsql_free_rows(rows)
	}
	return nil, nil
}

const (
	TYPE_INT int = iota + 1
	TYPE_FLOAT
	TYPE_TEXT
	TYPE_BLOB
	TYPE_NULL
)

func newRows(nativePtr C.libsql_rows_t) *rows {
	columnCount := int(C.libsql_column_count(nativePtr))
	columnTypes := make([]int, 0, columnCount)
	for i := 0; i < columnCount; i++ {
		columnType := int(C.libsql_column_type(nativePtr, C.int(i)))
		columnTypes = append(columnTypes, columnType)
	}
	return &rows{nativePtr, columnTypes}
}

type rows struct {
	nativePtr   C.libsql_rows_t
	columnTypes []int
}

func (r *rows) Columns() []string {
	if r.nativePtr == nil {
		return nil
	}
	columns := make([]string, len(r.columnTypes))
	for i := 0; i < len(r.columnTypes); i++ {
		ptr := C.libsql_column_name(r.nativePtr, C.int(i))
		columns[i] = C.GoString(ptr)
		C.libsql_free_string(ptr)
	}
	return columns
}

func (r *rows) Close() error {
	if r.nativePtr != nil {
		C.libsql_free_rows(r.nativePtr)
		r.nativePtr = nil
	}
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.nativePtr == nil {
		return io.EOF
	}
	row := C.libsql_next_row(r.nativePtr)
	if row == nil {
		r.Close()
		return io.EOF
	}
	defer C.libsql_free_row(row)
	count := len(dest)
	if count > len(r.columnTypes) {
		count = len(r.columnTypes)
	}
	for i := 0; i < count; i++ {
		switch r.columnTypes[i] {
		case TYPE_NULL:
			dest[i] = nil
		case TYPE_INT:
			dest[i] = int64(C.libsql_get_int(row, C.int(i)))
		case TYPE_FLOAT:
			dest[i] = float64(C.libsql_get_float(row, C.int(i)))
		case TYPE_BLOB:
			nativeBlob := C.libsql_get_blob(row, C.int(i))
			dest[i] = C.GoBytes(unsafe.Pointer(nativeBlob.ptr), C.int(nativeBlob.len))
			C.libsql_free_blob(nativeBlob)
		case TYPE_TEXT:
			ptr := C.libsql_get_string(row, C.int(i))
			dest[i] = C.GoString(ptr)
			C.libsql_free_string(ptr)
		}
	}
	return nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rowsNativePtr := c.execute(query)
	if rowsNativePtr == nil {
		return nil, fmt.Errorf("failed to execute query")
	}
	return newRows(rowsNativePtr), nil
}
