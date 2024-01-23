//go:build cgo
// +build cgo

package libsql

/*
#cgo CFLAGS: -I../c/include
#cgo LDFLAGS: -L../../target/debug
#cgo LDFLAGS: -lsql_experimental
#cgo LDFLAGS: -L../../libsql-sqlite3/.libs
#cgo LDFLAGS: -lsqlite3
#cgo LDFLAGS: -lm
#include <libsql.h>
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
	"unsafe"
)

func init() {
	sql.Register("libsql", driver{})
}

func NewEmbeddedReplicaConnector(dbPath, primaryUrl, authToken string) (*Connector, error) {
	return openEmbeddedReplicaConnector(dbPath, primaryUrl, authToken, 0)
}

func NewEmbeddedReplicaConnectorWithAutoSync(dbPath, primaryUrl, authToken string, syncInterval time.Duration) (*Connector, error) {
	return openEmbeddedReplicaConnector(dbPath, primaryUrl, authToken, syncInterval)
}

type driver struct{}

func (d driver) Open(dbAddress string) (sqldriver.Conn, error) {
	connector, err := d.OpenConnector(dbAddress)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (d driver) OpenConnector(dbAddress string) (sqldriver.Connector, error) {
	if strings.HasPrefix(dbAddress, ":memory:") {
		return openLocalConnector(dbAddress)
	}
	u, err := url.Parse(dbAddress)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "file":
		return openLocalConnector(dbAddress)
	case "http":
		fallthrough
	case "https":
		fallthrough
	case "libsql":
		authToken := u.Query().Get("authToken")
		u.RawQuery = ""
		return openRemoteConnector(u.String(), authToken)
	}
	return nil, fmt.Errorf("unsupported URL scheme: %s\nThis driver supports only URLs that start with libsql://, file://, https:// or http://", u.Scheme)
}

func libsqlSync(nativeDbPtr C.libsql_database_t) error {
	var errMsg *C.char
	statusCode := C.libsql_sync(nativeDbPtr, &errMsg)
	if statusCode != 0 {
		return libsqlError("failed to sync database ", statusCode, errMsg)
	}
	return nil
}

func openLocalConnector(dbPath string) (*Connector, error) {
	nativeDbPtr, err := libsqlOpenLocal(dbPath)
	if err != nil {
		return nil, err
	}
	return &Connector{nativeDbPtr: nativeDbPtr}, nil
}

func openRemoteConnector(primaryUrl, authToken string) (*Connector, error) {
	nativeDbPtr, err := libsqlOpenRemote(primaryUrl, authToken)
	if err != nil {
		return nil, err
	}
	return &Connector{nativeDbPtr: nativeDbPtr}, nil
}

func openEmbeddedReplicaConnector(dbPath, primaryUrl, authToken string, syncInterval time.Duration) (*Connector, error) {
	var closeCh chan struct{}
	var closeAckCh chan struct{}
	nativeDbPtr, err := libsqlOpenWithSync(dbPath, primaryUrl, authToken)
	if err != nil {
		return nil, err
	}
	if err := libsqlSync(nativeDbPtr); err != nil {
		C.libsql_close(nativeDbPtr)
		return nil, err
	}
	if syncInterval != 0 {
		closeCh = make(chan struct{}, 1)
		closeAckCh = make(chan struct{}, 1)
		go func() {
			for {
				timerCh := make(chan struct{}, 1)
				go func() {
					time.Sleep(syncInterval)
					timerCh <- struct{}{}
				}()
				select {
				case <-closeCh:
					closeAckCh <- struct{}{}
					return
				case <-timerCh:
					if err := libsqlSync(nativeDbPtr); err != nil {
						fmt.Println(err)
					}
				}
			}
		}()
	}
	if err != nil {
		return nil, err
	}
	return &Connector{nativeDbPtr: nativeDbPtr, closeCh: closeCh, closeAckCh: closeAckCh}, nil
}

type Connector struct {
	nativeDbPtr C.libsql_database_t
	closeCh     chan<- struct{}
	closeAckCh  <-chan struct{}
}

func (c *Connector) Sync() error {
	return libsqlSync(c.nativeDbPtr)
}

func (c *Connector) Close() error {
	if c.closeCh != nil {
		c.closeCh <- struct{}{}
		<-c.closeAckCh
	}
	C.libsql_close(c.nativeDbPtr)
	return nil
}

func (c *Connector) Connect(ctx context.Context) (sqldriver.Conn, error) {
	nativeConnPtr, err := libsqlConnect(c.nativeDbPtr)
	if err != nil {
		return nil, err
	}
	return &conn{nativePtr: nativeConnPtr}, nil
}

func (c *Connector) Driver() sqldriver.Driver {
	return driver{}
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

func libsqlOpenLocal(dataSourceName string) (C.libsql_database_t, error) {
	connectionString := C.CString(dataSourceName)
	defer C.free(unsafe.Pointer(connectionString))

	var db C.libsql_database_t
	var errMsg *C.char
	statusCode := C.libsql_open_file(connectionString, &db, &errMsg)
	if statusCode != 0 {
		return nil, libsqlError(fmt.Sprint("failed to open local database ", dataSourceName), statusCode, errMsg)
	}
	return db, nil
}

func libsqlOpenRemote(url, authToken string) (C.libsql_database_t, error) {
	connectionString := C.CString(url)
	defer C.free(unsafe.Pointer(connectionString))
	authTokenNativeString := C.CString(authToken)
	defer C.free(unsafe.Pointer(authTokenNativeString))

	var db C.libsql_database_t
	var errMsg *C.char
	statusCode := C.libsql_open_remote(connectionString, authTokenNativeString, &db, &errMsg)
	if statusCode != 0 {
		return nil, libsqlError(fmt.Sprint("failed to open remote database ", url), statusCode, errMsg)
	}
	return db, nil
}

func libsqlOpenWithSync(dbPath, primaryUrl, authToken string) (C.libsql_database_t, error) {
	dbPathNativeString := C.CString(dbPath)
	defer C.free(unsafe.Pointer(dbPathNativeString))
	primaryUrlNativeString := C.CString(primaryUrl)
	defer C.free(unsafe.Pointer(primaryUrlNativeString))
	authTokenNativeString := C.CString(authToken)
	defer C.free(unsafe.Pointer(authTokenNativeString))

	var db C.libsql_database_t
	var errMsg *C.char
	statusCode := C.libsql_open_sync(dbPathNativeString, primaryUrlNativeString, authTokenNativeString, &db, &errMsg)
	if statusCode != 0 {
		return nil, libsqlError(fmt.Sprintf("failed to open database %s %s", dbPath, primaryUrl), statusCode, errMsg)
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

type conn struct {
	nativePtr C.libsql_connection_t
}

func (c *conn) Prepare(query string) (sqldriver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) Begin() (sqldriver.Tx, error) {
	return c.BeginTx(context.Background(), sqldriver.TxOptions{})
}

func (c *conn) Close() error {
	C.libsql_disconnect(c.nativePtr)
	return nil
}

func (c *conn) PrepareContext(ctx context.Context, query string) (sqldriver.Stmt, error) {
	return nil, fmt.Errorf("prepare() is not implemented")
}

func (c *conn) BeginTx(ctx context.Context, opts sqldriver.TxOptions) (sqldriver.Tx, error) {
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

func (c *conn) ExecContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Result, error) {
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
	columns := make([]string, columnCount)
	for i := 0; i < columnCount; i++ {
		var ptr *C.char
		var errMsg *C.char
		statusCode := C.libsql_column_name(nativePtr, C.int(i), &ptr, &errMsg)
		if statusCode != 0 {
			return nil, libsqlError(fmt.Sprint("failed to get column name for index ", i), statusCode, errMsg)
		}
		columns[i] = C.GoString(ptr)
		C.libsql_free_string(ptr)
	}
	return &rows{nativePtr, nil, columns}, nil
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

func (r *rows) Next(dest []sqldriver.Value) error {
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
	if len(r.columnTypes) == 0 {
		columnCount := len(r.columnNames)
		columnTypes := make([]int, columnCount)
		for i := 0; i < columnCount; i++ {
			var columnType C.int
			var errMsg *C.char
			statusCode := C.libsql_column_type(r.nativePtr, C.int(i), &columnType, &errMsg)
			if statusCode != 0 {
				return libsqlError(fmt.Sprint("failed to get column type for index ", i), statusCode, errMsg)
			}
			columnTypes[i] = int(columnType)
		}
		r.columnTypes = columnTypes
	}
	count := len(dest)
	if count > len(r.columnNames) {
		count = len(r.columnNames)
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
			var value C.double
			var errMsg *C.char
			statusCode := C.libsql_get_float(row, C.int(i), &value, &errMsg)
			if statusCode != 0 {
				return libsqlError(fmt.Sprint("failed to get float for column ", i), statusCode, errMsg)
			}
			dest[i] = float64(value)
		case TYPE_BLOB:
			var nativeBlob C.blob
			var errMsg *C.char
			statusCode := C.libsql_get_blob(row, C.int(i), &nativeBlob, &errMsg)
			if statusCode != 0 {
				return libsqlError(fmt.Sprint("failed to get blob for column ", i), statusCode, errMsg)
			}
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

func (c *conn) QueryContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	rowsNativePtr, err := c.execute(query)
	if err != nil {
		return nil, err
	}
	return newRows(rowsNativePtr)
}
