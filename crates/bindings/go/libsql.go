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
	var conn C.libsql_connection_t
	var errMsg *C.char
	statusCode := C.libsql_connect(db, &conn, &errMsg)
	if statusCode != 0 {
		return nil, libsqlError("failed to connect to database", statusCode, errMsg)
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

func (c *conn) execute(query string) (C.libsql_rows_t, error) {
	queryCString := C.CString(query)
	defer C.free(unsafe.Pointer(queryCString))

	var rows C.libsql_rows_t
	var errMsg *C.char
	statusCode := C.libsql_execute(c.nativePtr, queryCString, &rows, &errMsg)
	if statusCode != 0 {
		return nil, libsqlError(fmt.Sprint("failed to execute query ", query), statusCode, errMsg)
	}
	return rows, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	rows, err := c.execute(query)
	if err != nil {
		return nil, err
	}
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

func newRows(nativePtr C.libsql_rows_t) (*rows, error) {
	if nativePtr == nil {
		return &rows{nil, nil, nil}, nil
	}
	columnCount := int(C.libsql_column_count(nativePtr))
	columnTypes := make([]int, columnCount)
	for i := 0; i < columnCount; i++ {
		var columnType C.int
		var errMsg *C.char
		statusCode := C.libsql_column_type(nativePtr, C.int(i), &columnType, &errMsg)
		if statusCode != 0 {
			return nil, libsqlError(fmt.Sprint("failed to get column type for index ", i), statusCode, errMsg)
		}
		columnTypes[i] = int(columnType)
	}
	columns := make([]string, len(columnTypes))
	for i := 0; i < len(columnTypes); i++ {
		var ptr *C.char
		var errMsg *C.char
		statusCode := C.libsql_column_name(nativePtr, C.int(i), &ptr, &errMsg)
		if statusCode != 0 {
			return nil, libsqlError(fmt.Sprint("failed to get column name for index ", i), statusCode, errMsg)
		}
		columns[i] = C.GoString(ptr)
		C.libsql_free_string(ptr)
	}
	return &rows{nativePtr, columnTypes, columns}, nil
}

type rows struct {
	nativePtr   C.libsql_rows_t
	columnTypes []int
	columnNames []string
}

func (r *rows) Columns() []string {
	return r.columnNames
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
	var row C.libsql_row_t
	var errMsg *C.char
	statusCode := C.libsql_next_row(r.nativePtr, &row, &errMsg)
	if statusCode != 0 {
		return libsqlError("failed to get next row", statusCode, errMsg)
	}
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
			var value C.longlong
			var errMsg *C.char
			statusCode := C.libsql_get_int(row, C.int(i), &value, &errMsg)
			if statusCode != 0 {
				return libsqlError(fmt.Sprint("failed to get integer for column ", i), statusCode, errMsg)
			}
			dest[i] = int64(value)
		case TYPE_FLOAT:
			dest[i] = float64(C.libsql_get_float(row, C.int(i)))
		case TYPE_BLOB:
			nativeBlob := C.libsql_get_blob(row, C.int(i))
			dest[i] = C.GoBytes(unsafe.Pointer(nativeBlob.ptr), C.int(nativeBlob.len))
			C.libsql_free_blob(nativeBlob)
		case TYPE_TEXT:
			var ptr *C.char
			var errMsg *C.char
			statusCode := C.libsql_get_string(row, C.int(i), &ptr, &errMsg)
			if statusCode != 0 {
				return libsqlError(fmt.Sprint("failed to get string for column ", i), statusCode, errMsg)
			}
			dest[i] = C.GoString(ptr)
			C.libsql_free_string(ptr)
		}
	}
	return nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rowsNativePtr, err := c.execute(query)
	if err != nil {
		return nil, err
	}
	return newRows(rowsNativePtr)
}
