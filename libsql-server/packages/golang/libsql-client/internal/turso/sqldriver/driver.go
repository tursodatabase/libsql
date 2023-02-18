package tursodriver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"

	tursohttp "github.com/libsql/sqld/tree/main/packages/golang/libsql-client/internal/turso/http"
)

type tursoResult struct {
	id      int64
	changes int64
}

func (r *tursoResult) LastInsertId() (int64, error) {
	return r.id, nil
}

func (r *tursoResult) RowsAffected() (int64, error) {
	return r.changes, nil
}

type tursoRows struct {
	result        *tursohttp.ResultSet
	currentRowIdx int
}

func (r *tursoRows) Columns() []string {
	return r.result.Columns
}

func (r *tursoRows) Close() error {
	return nil
}

func (r *tursoRows) Next(dest []driver.Value) error {
	if r.currentRowIdx == len(r.result.Rows) {
		return io.EOF
	}
	count := len(r.result.Rows[r.currentRowIdx])
	for idx := 0; idx < count; idx++ {
		dest[idx] = r.result.Rows[r.currentRowIdx][idx]
	}
	r.currentRowIdx++
	return nil
}

type tursoConn struct {
	url string
}

func TursoConnect(url string) *tursoConn {
	return &tursoConn{url}
}

func (c *tursoConn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("Prepare method not implemented")
}

func (c *tursoConn) Close() error {
	return nil
}

func (c *tursoConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("Begin method not implemented")
}

func (c *tursoConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	_, err := tursohttp.CallTurso(c.url, query)
	if err != nil {
		return nil, err
	}
	return &tursoResult{0, 0}, nil
}

func (c *tursoConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rs, err := tursohttp.CallTurso(c.url, query)
	if err != nil {
		return nil, err
	}
	return &tursoRows{rs, 0}, nil
}
