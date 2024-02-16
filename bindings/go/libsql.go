//go:build cgo
// +build cgo

package libsql

/*
#cgo CFLAGS: -I../c/include
#cgo LDFLAGS: -L../../target/release
#cgo LDFLAGS: -lsql_experimental
#cgo LDFLAGS: -lm
#include <libsql.h>
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/libsql/sqlite-antlr4-parser/sqliteparser"
	"github.com/libsql/sqlite-antlr4-parser/sqliteparserutils"
	"io"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unsafe"
)

func init() {
	sql.Register("libsql", driver{})
}

type config struct {
	authToken      *string
	readYourWrites *bool
	encryptionKey  *string
	syncInterval   *time.Duration
}

type Option interface {
	apply(*config) error
}

type option func(*config) error

func (o option) apply(c *config) error {
	return o(c)
}

func WithAuthToken(authToken string) Option {
	return option(func(o *config) error {
		if o.authToken != nil {
			return fmt.Errorf("authToken already set")
		}
		if authToken == "" {
			return fmt.Errorf("authToken must not be empty")
		}
		o.authToken = &authToken
		return nil
	})
}

func WithReadYourWrites(readYourWrites bool) Option {
	return option(func(o *config) error {
		if o.readYourWrites != nil {
			return fmt.Errorf("read your writes already set")
		}
		o.readYourWrites = &readYourWrites
		return nil
	})
}

func WithEncryption(key string) Option {
	return option(func(o *config) error {
		if o.encryptionKey != nil {
			return fmt.Errorf("encryption key already set")
		}
		if key == "" {
			return fmt.Errorf("encryption key must not be empty")
		}
		o.encryptionKey = &key
		return nil
	})
}

func WithSyncInterval(interval time.Duration) Option {
	return option(func(o *config) error {
		if o.syncInterval != nil {
			return fmt.Errorf("sync interval already set")
		}
		o.syncInterval = &interval
		return nil
	})
}

func NewEmbeddedReplicaConnector(dbPath string, primaryUrl string, opts ...Option) (*Connector, error) {
	var config config
	errs := make([]error, 0, len(opts))
	for _, opt := range opts {
		if err := opt.apply(&config); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	authToken := ""
	if config.authToken != nil {
		authToken = *config.authToken
	}
	readYourWrites := true
	if config.readYourWrites != nil {
		readYourWrites = *config.readYourWrites
	}
	encryptionKey := ""
	if config.encryptionKey != nil {
		encryptionKey = *config.encryptionKey
	}
	syncInterval := time.Duration(0)
	if config.syncInterval != nil {
		syncInterval = *config.syncInterval
	}
	return openEmbeddedReplicaConnector(dbPath, primaryUrl, authToken, readYourWrites, encryptionKey, syncInterval)
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
	return nil, fmt.Errorf("unsupported URL scheme: %s\nThis driver supports only URLs that start with libsql://, file:, https:// or http://", u.Scheme)
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

func openEmbeddedReplicaConnector(dbPath, primaryUrl, authToken string, readYourWrites bool, encryptionKey string, syncInterval time.Duration) (*Connector, error) {
	var closeCh chan struct{}
	var closeAckCh chan struct{}
	nativeDbPtr, err := libsqlOpenWithSync(dbPath, primaryUrl, authToken, readYourWrites, encryptionKey)
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
		c.closeCh = nil
		c.closeAckCh = nil
	}
	if c.nativeDbPtr != nil {
		C.libsql_close(c.nativeDbPtr)
	}
	c.nativeDbPtr = nil
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

func libsqlOpenWithSync(dbPath, primaryUrl, authToken string, readYourWrites bool, encryptionKey string) (C.libsql_database_t, error) {
	dbPathNativeString := C.CString(dbPath)
	defer C.free(unsafe.Pointer(dbPathNativeString))
	primaryUrlNativeString := C.CString(primaryUrl)
	defer C.free(unsafe.Pointer(primaryUrlNativeString))
	authTokenNativeString := C.CString(authToken)
	defer C.free(unsafe.Pointer(authTokenNativeString))

	var readYourWritesNative C.char = 0
	if readYourWrites {
		readYourWritesNative = 1
	}
	var encrytionKeyNativeString *C.char
	if encryptionKey != "" {
		encrytionKeyNativeString = C.CString(encryptionKey)
		defer C.free(unsafe.Pointer(encrytionKeyNativeString))
	}

	var db C.libsql_database_t
	var errMsg *C.char
	statusCode := C.libsql_open_sync(dbPathNativeString, primaryUrlNativeString, authTokenNativeString, readYourWritesNative, encrytionKeyNativeString, &db, &errMsg)
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

type ParamsInfo struct {
	NamedParameters           []string
	PositionalParametersCount int
}

func isPositionalParameter(param string) (ok bool, err error) {
	re := regexp.MustCompile(`\?([0-9]*).*`)
	match := re.FindSubmatch([]byte(param))
	if match == nil {
		return false, nil
	}

	posS := string(match[1])
	if posS == "" {
		return true, nil
	}

	return true, fmt.Errorf("unsuppoted positional parameter. This driver does not accept positional parameters with indexes (like ?<number>)")
}

func removeParamPrefix(paramName string) (string, error) {
	if paramName[0] == ':' || paramName[0] == '@' || paramName[0] == '$' {
		return paramName[1:], nil
	}
	return "", fmt.Errorf("all named parameters must start with ':', or '@' or '$'")
}

func extractParameters(stmt string) (nameParams []string, positionalParamsCount int, err error) {
	statementStream := antlr.NewInputStream(stmt)
	sqliteparser.NewSQLiteLexer(statementStream)
	lexer := sqliteparser.NewSQLiteLexer(statementStream)

	allTokens := lexer.GetAllTokens()

	nameParamsSet := make(map[string]bool)

	for _, token := range allTokens {
		tokenType := token.GetTokenType()
		if tokenType == sqliteparser.SQLiteLexerBIND_PARAMETER {
			parameter := token.GetText()

			isPositionalParameter, err := isPositionalParameter(parameter)
			if err != nil {
				return []string{}, 0, err
			}

			if isPositionalParameter {
				positionalParamsCount++
			} else {
				paramWithoutPrefix, err := removeParamPrefix(parameter)
				if err != nil {
					return []string{}, 0, err
				} else {
					nameParamsSet[paramWithoutPrefix] = true
				}
			}
		}
	}
	nameParams = make([]string, 0, len(nameParamsSet))
	for k := range nameParamsSet {
		nameParams = append(nameParams, k)
	}

	return nameParams, positionalParamsCount, nil
}

func parseStatement(sql string) ([]string, []ParamsInfo, error) {
	stmts, _ := sqliteparserutils.SplitStatement(sql)

	stmtsParams := make([]ParamsInfo, len(stmts))
	for idx, stmt := range stmts {
		nameParams, positionalParamsCount, err := extractParameters(stmt)
		if err != nil {
			return nil, nil, err
		}
		stmtsParams[idx] = ParamsInfo{nameParams, positionalParamsCount}
	}
	return stmts, stmtsParams, nil
}

func (c *conn) PrepareContext(ctx context.Context, query string) (sqldriver.Stmt, error) {
	stmts, paramInfos, err := parseStatement(query)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("only one statement is supported got %d", len(stmts))
	}
	numInput := -1
	if len(paramInfos[0].NamedParameters) == 0 {
		numInput = paramInfos[0].PositionalParametersCount
	}
	return &stmt{c, query, numInput}, nil
}

func (c *conn) BeginTx(ctx context.Context, opts sqldriver.TxOptions) (sqldriver.Tx, error) {
	if opts.ReadOnly {
		return nil, fmt.Errorf("read only transactions are not supported")
	}
	if opts.Isolation != sqldriver.IsolationLevel(sql.LevelDefault) {
		return nil, fmt.Errorf("isolation level %d is not supported", opts.Isolation)
	}
	_, err := c.ExecContext(ctx, "BEGIN", nil)
	if err != nil {
		return nil, err
	}
	return &tx{c}, nil
}

func (c *conn) executeNoArgs(query string, exec bool) (C.libsql_rows_t, error) {
	queryCString := C.CString(query)
	defer C.free(unsafe.Pointer(queryCString))

	var rows C.libsql_rows_t
	var errMsg *C.char
	var statusCode C.int
	if exec {
		statusCode = C.libsql_execute(c.nativePtr, queryCString, &errMsg)
	} else {
		statusCode = C.libsql_query(c.nativePtr, queryCString, &rows, &errMsg)
	}
	if statusCode != 0 {
		return nil, libsqlError(fmt.Sprint("failed to execute query ", query), statusCode, errMsg)
	}
	return rows, nil
}

func (c *conn) execute(query string, args []sqldriver.NamedValue, exec bool) (C.libsql_rows_t, error) {
	if len(args) == 0 {
		return c.executeNoArgs(query, exec)
	}
	queryCString := C.CString(query)
	defer C.free(unsafe.Pointer(queryCString))

	var stmt C.libsql_stmt_t
	var errMsg *C.char
	statusCode := C.libsql_prepare(c.nativePtr, queryCString, &stmt, &errMsg)
	if statusCode != 0 {
		return nil, libsqlError(fmt.Sprint("failed to prepare query ", query), statusCode, errMsg)
	}
	defer C.libsql_free_stmt(stmt)

	for _, arg := range args {
		var errMsg *C.char
		var statusCode C.int
		idx := arg.Ordinal
		switch arg.Value.(type) {
		case int64:
			statusCode = C.libsql_bind_int(stmt, C.int(idx), C.longlong(arg.Value.(int64)), &errMsg)
		case float64:
			statusCode = C.libsql_bind_float(stmt, C.int(idx), C.double(arg.Value.(float64)), &errMsg)
		case []byte:
			blob := arg.Value.([]byte)
			nativeBlob := C.CBytes(blob)
			statusCode = C.libsql_bind_blob(stmt, C.int(idx), (*C.uchar)(nativeBlob), C.int(len(blob)), &errMsg)
			C.free(nativeBlob)
		case string:
			valueStr := C.CString(arg.Value.(string))
			statusCode = C.libsql_bind_string(stmt, C.int(idx), valueStr, &errMsg)
			C.free(unsafe.Pointer(valueStr))
		case nil:
			statusCode = C.libsql_bind_null(stmt, C.int(idx), &errMsg)
		case bool:
			var valueInt int
			if arg.Value.(bool) {
				valueInt = 1
			} else {
				valueInt = 0
			}
			statusCode = C.libsql_bind_int(stmt, C.int(idx), C.longlong(valueInt), &errMsg)
		default:
			return nil, fmt.Errorf("unsupported type %T", arg.Value)
		}
		if statusCode != 0 {
			return nil, libsqlError(fmt.Sprintf("failed to bind argument no. %d with value %v and type %T", idx, arg.Value, arg.Value), statusCode, errMsg)
		}
	}

	var rows C.libsql_rows_t
	if exec {
		statusCode = C.libsql_execute_stmt(stmt, &errMsg)
	} else {
		statusCode = C.libsql_query_stmt(stmt, &rows, &errMsg)
	}
	if statusCode != 0 {
		return nil, libsqlError(fmt.Sprint("failed to execute query ", query), statusCode, errMsg)
	}
	return rows, nil
}

type execResult struct {
	id      int64
	changes int64
}

func (r execResult) LastInsertId() (int64, error) {
	return r.id, nil
}

func (r execResult) RowsAffected() (int64, error) {
	return r.changes, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Result, error) {
	rows, err := c.execute(query, args, true)
	if err != nil {
		return nil, err
	}
	id := int64(C.libsql_last_insert_rowid(c.nativePtr))
	changes := int64(C.libsql_changes(c.nativePtr))
	if rows != nil {
		C.libsql_free_rows(rows)
	}
	return execResult{id, changes}, nil
}

type stmt struct {
	conn     *conn
	sql      string
	numInput int
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return s.numInput
}

func convertToNamed(args []sqldriver.Value) []sqldriver.NamedValue {
	if len(args) == 0 {
		return nil
	}
	result := make([]sqldriver.NamedValue, 0, len(args))
	for idx := range args {
		result = append(result, sqldriver.NamedValue{Ordinal: idx, Value: args[idx]})
	}
	return result
}

func (s *stmt) Exec(args []sqldriver.Value) (sqldriver.Result, error) {
	return s.ExecContext(context.Background(), convertToNamed(args))
}

func (s *stmt) Query(args []sqldriver.Value) (sqldriver.Rows, error) {
	return s.QueryContext(context.Background(), convertToNamed(args))
}

func (s *stmt) ExecContext(ctx context.Context, args []sqldriver.NamedValue) (sqldriver.Result, error) {
	return s.conn.ExecContext(ctx, s.sql, args)
}

func (s *stmt) QueryContext(ctx context.Context, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	return s.conn.QueryContext(ctx, s.sql, args)
}

type tx struct {
	conn *conn
}

func (t tx) Commit() error {
	_, err := t.conn.ExecContext(context.Background(), "COMMIT", nil)
	return err
}

func (t tx) Rollback() error {
	_, err := t.conn.ExecContext(context.Background(), "ROLLBACK", nil)
	return err
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
		return &rows{nil, nil}, nil
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
	return &rows{nativePtr, columns}, nil
}

type rows struct {
	nativePtr   C.libsql_rows_t
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
	count := len(dest)
	if count > len(r.columnNames) {
		count = len(r.columnNames)
	}
	for i := 0; i < count; i++ {
		var columnType C.int
		var errMsg *C.char
		statusCode := C.libsql_column_type(r.nativePtr, row, C.int(i), &columnType, &errMsg)
		if statusCode != 0 {
			return libsqlError(fmt.Sprint("failed to get column type for index ", i), statusCode, errMsg)
		}

		switch int(columnType) {
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
			str := C.GoString(ptr)
			C.libsql_free_string(ptr)
			for _, format := range []string{
				"2006-01-02 15:04:05.999999999-07:00",
				"2006-01-02T15:04:05.999999999-07:00",
				"2006-01-02 15:04:05.999999999",
				"2006-01-02T15:04:05.999999999",
				"2006-01-02 15:04:05",
				"2006-01-02T15:04:05",
				"2006-01-02 15:04",
				"2006-01-02T15:04",
				"2006-01-02",
			} {
				if t, err := time.ParseInLocation(format, str, time.UTC); err == nil {
					dest[i] = t
					return nil
				}
			}
			dest[i] = str
		}
	}
	return nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	rowsNativePtr, err := c.execute(query, args, false)
	if err != nil {
		return nil, err
	}
	return newRows(rowsNativePtr)
}
