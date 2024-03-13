package libsql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"gotest.tools/assert"
	"io"
	"math/rand"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

type T struct {
	*testing.T
}

func (t T) FatalWithMsg(msg string) {
	t.Log(string(debug.Stack()))
	t.Fatal(msg)
}

func (t T) FatalOnError(err error) {
	if err != nil {
		t.Log(string(debug.Stack()))
		t.Fatal(err)
	}
}

func getRemoteDb(t T) *Database {
	primaryUrl := os.Getenv("LIBSQL_PRIMARY_URL")
	if primaryUrl == "" {
		t.Skip("LIBSQL_PRIMARY_URL is not set")
		return nil
	}
	authToken := os.Getenv("LIBSQL_AUTH_TOKEN")
	db, err := sql.Open("libsql", primaryUrl+"?authToken="+authToken)
	t.FatalOnError(err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(func() {
		db.Close()
		cancel()
	})
	return &Database{db, nil, t, ctx}
}

func getEmbeddedDb(t T) *Database {
	primaryUrl := os.Getenv("LIBSQL_PRIMARY_URL")
	if primaryUrl == "" {
		t.Skip("LIBSQL_PRIMARY_URL is not set")
		return nil
	}
	authToken := os.Getenv("LIBSQL_AUTH_TOKEN")
	dir, err := os.MkdirTemp("", "libsql-*")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := dir + "/test.db"
	options := []Option{WithReadYourWrites(false)}
	if authToken != "" {
		options = append(options, WithAuthToken(authToken))
	}
	connector, err := NewEmbeddedReplicaConnector(dbPath, primaryUrl, options...)
	t.FatalOnError(err)
	db := sql.OpenDB(connector)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(func() {
		db.Close()
		connector.Close()
		cancel()
		defer os.RemoveAll(dir)
	})
	return &Database{db, connector, t, ctx}
}

type Database struct {
	*sql.DB
	connector *Connector
	t         T
	ctx       context.Context
}

func (db Database) exec(sql string, args ...any) sql.Result {
	res, err := db.ExecContext(db.ctx, sql, args...)
	db.t.FatalOnError(err)
	return res
}

func (db Database) query(sql string, args ...any) *sql.Rows {
	rows, err := db.QueryContext(db.ctx, sql, args...)
	db.t.FatalOnError(err)
	return rows
}

func (db Database) sync() {
	if db.connector != nil {
		db.connector.Sync()
	}
}

type Table struct {
	name string
	db   Database
}

func (db Database) createTable() Table {
	name := "test_" + fmt.Sprint(rand.Int()) + "_" + time.Now().Format("20060102150405")
	db.exec("CREATE TABLE " + name + " (a int, b int)")
	db.t.Cleanup(func() {
		db.exec("DROP TABLE " + name)
	})
	return Table{name, db}
}

func (db Database) assertTable(name string) {
	rows, err := db.QueryContext(db.ctx, "select 1 from "+name)
	db.t.FatalOnError(err)
	defer rows.Close()
}

func (t Table) insertRows(start, count int) {
	t.insertRowsInternal(start, count, func(i int) sql.Result {
		return t.db.exec("INSERT INTO " + t.name + " (a, b) VALUES (" + fmt.Sprint(i) + ", " + fmt.Sprint(i) + ")")
	})
}

func (t Table) insertRowsWithArgs(start, count int) {
	t.insertRowsInternal(start, count, func(i int) sql.Result {
		return t.db.exec("INSERT INTO "+t.name+" (a, b) VALUES (?, ?)", i, i)
	})
}

func (t Table) insertRowsInternal(start, count int, execFn func(i int) sql.Result) {
	for i := 0; i < count; i++ {
		execFn(i + start)
		//Uncomment once RowsAffected is implemented in libsql for remote only dbs
		//res := execFn(i + start)
		//affected, err := res.RowsAffected()
		//t.db.t.FatalOnError(err)
		//if affected != 1 {
		//	t.db.t.FatalWithMsg("expected 1 row affected")
		//}
	}
}

func (t Table) assertRowsCount(count int) {
	t.assertCount(count, func() *sql.Rows {
		return t.db.query("SELECT COUNT(*) FROM " + t.name)
	})
}

func (t Table) assertRowDoesNotExist(id int) {
	t.assertCount(0, func() *sql.Rows {
		return t.db.query("SELECT COUNT(*) FROM "+t.name+" WHERE a = ?", id)
	})
}

func (t Table) assertRowExists(id int) {
	t.assertCount(1, func() *sql.Rows {
		return t.db.query("SELECT COUNT(*) FROM "+t.name+" WHERE a = ?", id)
	})
}

func (t Table) assertCount(expectedCount int, queryFn func() *sql.Rows) {
	rows := queryFn()
	defer rows.Close()
	if !rows.Next() {
		t.db.t.FatalWithMsg(fmt.Sprintf("expected at least one row: %v", rows.Err()))
	}
	var rowCount int
	t.db.t.FatalOnError(rows.Scan(&rowCount))
	if rowCount != expectedCount {
		t.db.t.FatalWithMsg(fmt.Sprintf("expected %d rows, got %d", expectedCount, rowCount))
	}
}

func (t Table) beginTx() Tx {
	tx, err := t.db.BeginTx(t.db.ctx, nil)
	t.db.t.FatalOnError(err)
	return Tx{tx, t, nil}
}

func (t Table) beginTxWithContext(ctx context.Context) Tx {
	tx, err := t.db.BeginTx(ctx, nil)
	t.db.t.FatalOnError(err)
	return Tx{tx, t, &ctx}
}

func (t Table) prepareInsertStmt() PreparedStmt {
	stmt, err := t.db.Prepare("INSERT INTO " + t.name + " (a, b) VALUES (?, ?)")
	t.db.t.FatalOnError(err)
	return PreparedStmt{stmt, t}
}

type PreparedStmt struct {
	*sql.Stmt
	t Table
}

func (s PreparedStmt) exec(args ...any) sql.Result {
	res, err := s.ExecContext(s.t.db.ctx, args...)
	s.t.db.t.FatalOnError(err)
	return res
}

type Tx struct {
	*sql.Tx
	t   Table
	ctx *context.Context
}

func (t Tx) context() context.Context {
	if t.ctx != nil {
		return *t.ctx
	}
	return t.t.db.ctx
}

func (t Tx) exec(sql string, args ...any) sql.Result {
	res, err := t.ExecContext(t.context(), sql, args...)
	t.t.db.t.FatalOnError(err)
	return res
}

func (t Tx) query(sql string, args ...any) *sql.Rows {
	rows, err := t.QueryContext(t.context(), sql, args...)
	t.t.db.t.FatalOnError(err)
	return rows
}

func (t Tx) insertRows(start, count int) {
	t.t.insertRowsInternal(start, count, func(i int) sql.Result {
		return t.exec("INSERT INTO " + t.t.name + " (a, b) VALUES (" + fmt.Sprint(i) + ", '" + fmt.Sprint(i) + "')")
	})
}

func (t Tx) insertRowsWithArgs(start, count int) {
	t.t.insertRowsInternal(start, count, func(i int) sql.Result {
		return t.exec("INSERT INTO "+t.t.name+" (a, b) VALUES (?, ?)", i, fmt.Sprint(i))
	})
}

func (t Tx) assertRowsCount(count int) {
	t.t.assertCount(count, func() *sql.Rows {
		return t.query("SELECT COUNT(*) FROM " + t.t.name)
	})
}

func (t Tx) assertRowDoesNotExist(id int) {
	t.t.assertCount(0, func() *sql.Rows {
		return t.query("SELECT COUNT(*) FROM "+t.t.name+" WHERE a = ?", id)
	})
}

func (t Tx) assertRowExists(id int) {
	t.t.assertCount(1, func() *sql.Rows {
		return t.query("SELECT COUNT(*) FROM "+t.t.name+" WHERE a = ?", id)
	})
}

func (t Tx) prepareInsertStmt() PreparedStmt {
	stmt, err := t.Prepare("INSERT INTO " + t.t.name + " (a, b) VALUES (?, ?)")
	t.t.db.t.FatalOnError(err)
	return PreparedStmt{stmt, t.t}
}

func executeSql(t *testing.T, primaryUrl, authToken, sql string) {
	type statement struct {
		Query string `json:"q"`
	}
	type postBody struct {
		Statements []statement `json:"statements"`
	}

	type resultSet struct {
		Columns []string `json:"columns"`
	}

	type httpErrObject struct {
		Message string `json:"message"`
	}

	type httpResults struct {
		Results *resultSet     `json:"results"`
		Error   *httpErrObject `json:"error"`
	}

	type httpResultsAlternative struct {
		Results *resultSet `json:"results"`
		Error   string     `json:"error"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rawReq := postBody{}

	rawReq.Statements = append(rawReq.Statements, statement{Query: sql})

	body, err := json.Marshal(rawReq)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", primaryUrl+"", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatal("unexpected status code: ", resp.StatusCode)
	}
	var results []httpResults

	err = json.Unmarshal(body, &results)
	if err != nil {
		var alternativeResults []httpResultsAlternative
		errArray := json.Unmarshal(body, &alternativeResults)
		if errArray != nil {
			t.Fatal("failed to unmarshal response: ", err, errArray)
		}
		if alternativeResults[0].Error != "" {
			t.Fatal(errors.New(alternativeResults[0].Error))
		}
	} else {
		if results[0].Error != nil {
			t.Fatal(errors.New(results[0].Error.Message))
		}
		if results[0].Results == nil {
			t.Fatal(errors.New("no results"))
		}
	}
}

func insertRow(t *testing.T, dbUrl, authToken, tableName string, id int) {
	executeSql(t, dbUrl, authToken, fmt.Sprintf("INSERT INTO %s (id, name, gpa, cv) VALUES (%d, '%d', %d.5, randomblob(10));", tableName, id, id, id))
}

func insertRows(t *testing.T, dbUrl, authToken, tableName string, start, count int) {
	for i := 0; i < count; i++ {
		insertRow(t, dbUrl, authToken, tableName, start+i)
	}
}

func createTable(t *testing.T, dbPath, authToken string) string {
	tableName := fmt.Sprintf("test_%d", time.Now().UnixNano())
	executeSql(t, dbPath, authToken, fmt.Sprintf("CREATE TABLE %s (id INTEGER, name TEXT, gpa REAL, cv BLOB);", tableName))
	return tableName
}

func removeTable(t *testing.T, dbPath, authToken, tableName string) {
	executeSql(t, dbPath, authToken, fmt.Sprintf("DROP TABLE %s;", tableName))
}

func testSync(t *testing.T, connect func(dbPath, primaryUrl, authToken string) *Connector, sync func(connector *Connector)) {
	primaryUrl := os.Getenv("LIBSQL_PRIMARY_URL")
	if primaryUrl == "" {
		t.Skip("LIBSQL_PRIMARY_URL is not set")
		return
	}
	authToken := os.Getenv("LIBSQL_AUTH_TOKEN")
	tableName := createTable(t, primaryUrl, authToken)
	defer removeTable(t, primaryUrl, authToken, tableName)

	initialRowsCount := 5
	insertRows(t, primaryUrl, authToken, tableName, 0, initialRowsCount)
	dir, err := os.MkdirTemp("", "libsql-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	connector := connect(dir+"/test.db", primaryUrl, authToken)
	db := sql.OpenDB(connector)
	defer db.Close()

	iterCount := 2
	for iter := 0; iter < iterCount; iter++ {
		func() {
			rows, err := db.QueryContext(context.Background(), "SELECT NULL, id, name, gpa, cv FROM "+tableName)
			if err != nil {
				t.Fatal(err)
			}
			columns, err := rows.Columns()
			if err != nil {
				t.Fatal(err)
			}
			assert.DeepEqual(t, columns, []string{"NULL", "id", "name", "gpa", "cv"})
			types, err := rows.ColumnTypes()
			if err != nil {
				t.Fatal(err)
			}
			if len(types) != 5 {
				t.Fatal("types should be 5")
			}
			defer rows.Close()
			idx := 0
			for rows.Next() {
				if idx > initialRowsCount+iter {
					t.Fatal("idx should be <= ", initialRowsCount+iter)
				}
				var null any
				var id int
				var name string
				var gpa float64
				var cv []byte
				if err := rows.Scan(&null, &id, &name, &gpa, &cv); err != nil {
					t.Fatal(err)
				}
				if null != nil {
					t.Fatal("null should be nil")
				}
				if id != int(idx) {
					t.Fatal("id should be ", idx, " got ", id)
				}
				if name != fmt.Sprint(idx) {
					t.Fatal("name should be", idx)
				}
				if gpa != float64(idx)+0.5 {
					t.Fatal("gpa should be", float64(idx)+0.5)
				}
				if len(cv) != 10 {
					t.Fatal("cv should be 10 bytes")
				}
				idx++
			}
			if idx != initialRowsCount+iter {
				t.Fatal("idx should be ", initialRowsCount+iter, " got ", idx)
			}
		}()
		if iter+1 != iterCount {
			insertRow(t, primaryUrl, authToken, tableName, initialRowsCount+iter)
			sync(connector)
		}
	}
}

func TestAutoSync(t *testing.T) {
	syncInterval := 1 * time.Second
	testSync(t, func(dbPath, primaryUrl, authToken string) *Connector {
		options := []Option{WithReadYourWrites(false), WithSyncInterval(syncInterval)}
		if authToken != "" {
			options = append(options, WithAuthToken(authToken))
		}
		connector, err := NewEmbeddedReplicaConnector(dbPath, primaryUrl, options...)
		if err != nil {
			t.Fatal(err)
		}
		return connector
	}, func(_ *Connector) {
		time.Sleep(2 * syncInterval)
	})
}

func TestSync(t *testing.T) {
	testSync(t, func(dbPath, primaryUrl, authToken string) *Connector {
		options := []Option{WithReadYourWrites(false)}
		if authToken != "" {
			options = append(options, WithAuthToken(authToken))
		}
		connector, err := NewEmbeddedReplicaConnector(dbPath, primaryUrl, options...)
		if err != nil {
			t.Fatal(err)
		}
		return connector
	}, func(c *Connector) {
		if err := c.Sync(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestEncryption(tt *testing.T) {
	t := T{tt}
	primaryUrl := os.Getenv("LIBSQL_PRIMARY_URL")
	if primaryUrl == "" {
		t.Skip("LIBSQL_PRIMARY_URL is not set")
		return
	}
	authToken := os.Getenv("LIBSQL_AUTH_TOKEN")
	dir, err := os.MkdirTemp("", "libsql-*")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := dir + "/test.db"
	t.Cleanup(func() {
		defer os.RemoveAll(dir)
	})

	encryptionKey := "SuperSecretKey"
	table := "test_" + fmt.Sprint(rand.Int()) + "_" + time.Now().Format("20060102150405")

	options := []Option{WithReadYourWrites(false)}
	if authToken != "" {
		options = append(options, WithAuthToken(authToken))
	}
	connector, err := NewEmbeddedReplicaConnector(dbPath, primaryUrl, append(options, WithEncryption(encryptionKey))...)
	t.FatalOnError(err)
	db := sql.OpenDB(connector)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	_, err = db.ExecContext(ctx, "CREATE TABLE "+table+" (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		cancel()
		db.Close()
		connector.Close()
		t.FatalOnError(err)
	}
	_, err = db.ExecContext(ctx, "INSERT INTO "+table+" (id, name) VALUES (1, 'hello')")
	if err != nil {
		cancel()
		db.Close()
		connector.Close()
		t.FatalOnError(err)
	}
	err = db.Close()
	t.FatalOnError(err)
	err = connector.Close()
	t.FatalOnError(err)
	connector, err = NewEmbeddedReplicaConnector(dbPath, primaryUrl, append(options, WithEncryption(encryptionKey))...)
	t.FatalOnError(err)
	db = sql.OpenDB(connector)
	rows, err := db.QueryContext(ctx, "SELECT * FROM "+table)
	if err != nil {
		cancel()
		db.Close()
		connector.Close()
		t.FatalOnError(err)
	}
	defer rows.Close()
	if !rows.Next() {
		cancel()
		db.Close()
		connector.Close()
		t.Fatal("expected one row")
	}
	var id int
	var name string
	err = rows.Scan(&id, &name)
	if err != nil {
		cancel()
		db.Close()
		connector.Close()
		t.FatalOnError(err)
	}
	if id != 1 {
		cancel()
		db.Close()
		connector.Close()
		t.Fatal("id should be 1")
	}
	if name != "hello" {
		cancel()
		db.Close()
		connector.Close()
		t.Fatal("name should be hello")
	}
	err = rows.Close()
	t.FatalOnError(err)
	err = db.Close()
	t.FatalOnError(err)
	err = connector.Close()
	t.FatalOnError(err)
	connector, err = NewEmbeddedReplicaConnector(dbPath, primaryUrl, append(options, WithEncryption("WrongKey"))...)
	if err == nil {
		t.Fatal("using wrong encryption key should have failed")
	}
	if !strings.Contains(err.Error(), "SQLite error: file is not a database") {
		t.Fatal("using wrong encryption key should have failed with a different error")
	}
}

func TestExecAndQuery(t *testing.T) {
	db := getRemoteDb(T{t})
	testExecAndQuery(db)
}

func TestExecAndQueryEmbedded(t *testing.T) {
	db := getEmbeddedDb(T{t})
	testExecAndQuery(db)
}

func testExecAndQuery(db *Database) {
	if db == nil {
		return
	}
	table := db.createTable()
	table.insertRows(0, 10)
	table.insertRowsWithArgs(10, 10)
	db.sync()
	table.assertRowsCount(20)
	table.assertRowDoesNotExist(20)
	table.assertRowExists(0)
	table.assertRowExists(19)
}

func TestReadYourWrites(tt *testing.T) {
	t := T{tt}
	primaryUrl := os.Getenv("LIBSQL_PRIMARY_URL")
	if primaryUrl == "" {
		t.Skip("LIBSQL_PRIMARY_URL is not set")
		return
	}
	authToken := os.Getenv("LIBSQL_AUTH_TOKEN")
	dir, err := os.MkdirTemp("", "libsql-*")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := dir + "/test.db"
	options := []Option{}
	if authToken != "" {
		options = append(options, WithAuthToken(authToken))
	}
	connector, err := NewEmbeddedReplicaConnector(dbPath, primaryUrl, options...)
	t.FatalOnError(err)
	database := sql.OpenDB(connector)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(func() {
		database.Close()
		connector.Close()
		cancel()
		defer os.RemoveAll(dir)
	})
	db := &Database{database, connector, t, ctx}
	table := db.createTable()
	table.insertRows(0, 10)
	table.insertRowsWithArgs(10, 10)
	table.assertRowsCount(20)
	table.assertRowDoesNotExist(20)
	table.assertRowExists(0)
	table.assertRowExists(19)
}

func TestPreparedStatements(t *testing.T) {
	db := getRemoteDb(T{t})
	testPreparedStatements(db)
}

func TestPreparedStatementsEmbedded(t *testing.T) {
	db := getEmbeddedDb(T{t})
	testPreparedStatements(db)
}

func testPreparedStatements(db *Database) {
	if db == nil {
		return
	}
	table := db.createTable()
	stmt := table.prepareInsertStmt()
	stmt.exec(1, "1")
	db.t.FatalOnError(stmt.Close())
	db.sync()
	table.assertRowsCount(1)
	table.assertRowExists(1)
}

func TestTransaction(t *testing.T) {
	db := getRemoteDb(T{t})
	testTransaction(db)
}

func TestTransactionEmbedded(t *testing.T) {
	db := getEmbeddedDb(T{t})
	testTransaction(db)
}

func testTransaction(db *Database) {
	if db == nil {
		return
	}
	table := db.createTable()
	tx := table.beginTx()
	tx.insertRows(0, 10)
	tx.insertRowsWithArgs(10, 10)
	tx.assertRowsCount(20)
	tx.assertRowDoesNotExist(20)
	tx.assertRowExists(0)
	tx.assertRowExists(19)
	db.t.FatalOnError(tx.Commit())
	db.sync()
	table.assertRowsCount(20)
	table.assertRowDoesNotExist(20)
	table.assertRowExists(0)
	table.assertRowExists(19)
}

func TestMultiLineStatement(t *testing.T) {
	t.Skip("Make it work")
	db := getRemoteDb(T{t})
	if db == nil {
		return
	}
	db.exec("CREATE TABLE IF NOT EXISTS my_table (my_data TEXT); INSERT INTO my_table (my_data) VALUES ('hello');")
	t.Cleanup(func() {
		db.exec("DROP TABLE my_table")
	})
	table := Table{"my_table", *db}
	db.assertTable("my_table")
	table.assertRowsCount(1)
}

func TestPreparedStatementInTransaction(t *testing.T) {
	db := getRemoteDb(T{t})
	testPreparedStatementInTransaction(db)
}

func TestPreparedStatementInTransactionEmbedded(t *testing.T) {
	db := getEmbeddedDb(T{t})
	testPreparedStatementInTransaction(db)
}

func testPreparedStatementInTransaction(db *Database) {
	if db == nil {
		return
	}
	table := db.createTable()
	tx := table.beginTx()
	stmt := tx.prepareInsertStmt()
	stmt.exec(1, "1")
	db.t.FatalOnError(stmt.Close())
	tx.assertRowsCount(1)
	tx.assertRowExists(1)
	db.t.FatalOnError(tx.Commit())
	db.sync()
	table.assertRowsCount(1)
	table.assertRowExists(1)
}

func TestPreparedStatementInTransactionRollback(t *testing.T) {
	db := getRemoteDb(T{t})
	testPreparedStatementInTransactionRollback(db)
}

func TestPreparedStatementInTransactionRollbackEmbedded(t *testing.T) {
	db := getEmbeddedDb(T{t})
	testPreparedStatementInTransactionRollback(db)
}

func testPreparedStatementInTransactionRollback(db *Database) {
	if db == nil {
		return
	}
	table := db.createTable()
	tx := table.beginTx()
	stmt := tx.prepareInsertStmt()
	stmt.exec(1, "1")
	db.t.FatalOnError(stmt.Close())
	tx.assertRowsCount(1)
	tx.assertRowExists(1)
	db.t.FatalOnError(tx.Rollback())
	db.sync()
	table.assertRowsCount(0)
	table.assertRowDoesNotExist(1)
}

func TestCancelContext(t *testing.T) {
	db := getRemoteDb(T{t})
	testCancelContext(db)
}

func TestCancelContextEmbedded(t *testing.T) {
	db := getEmbeddedDb(T{t})
	testCancelContext(db)
}

func testCancelContext(db *Database) {
	if db == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
	if err == nil {
		db.t.FatalWithMsg("should have failed")
	}
	if !errors.Is(err, context.Canceled) {
		db.t.FatalWithMsg("should have failed with context.Canceled")
	}
}

func TestCancelContextWithTransaction(t *testing.T) {
	db := getRemoteDb(T{t})
	testCancelContextWithTransaction(db)
}

func TestCancelContextWithTransactionEmbedded(t *testing.T) {
	db := getEmbeddedDb(T{t})
	testCancelContextWithTransaction(db)
}

func testCancelContextWithTransaction(db *Database) {
	if db == nil {
		return
	}
	table := db.createTable()
	ctx, cancel := context.WithCancel(context.Background())
	tx := table.beginTxWithContext(ctx)
	tx.insertRows(0, 10)
	tx.insertRowsWithArgs(10, 10)
	tx.assertRowsCount(20)
	tx.assertRowDoesNotExist(20)
	tx.assertRowExists(0)
	tx.assertRowExists(19)
	// let's cancel the context before the commit
	cancel()
	err := tx.Commit()
	if err == nil {
		db.t.FatalWithMsg("should have failed")
	}
	if !errors.Is(err, context.Canceled) {
		db.t.FatalWithMsg("should have failed with context.Canceled")
	}
	// rolling back the transaction should not result in any error
	db.t.FatalOnError(tx.Rollback())
}

func TestTransactionRollback(t *testing.T) {
	db := getRemoteDb(T{t})
	testTransactionRollback(db)
}

func TestTransactionRollbackEmbedded(t *testing.T) {
	db := getEmbeddedDb(T{t})
	testTransactionRollback(db)
}

func testTransactionRollback(db *Database) {
	if db == nil {
		return
	}
	table := db.createTable()
	tx := table.beginTx()
	tx.insertRows(0, 10)
	tx.insertRowsWithArgs(10, 10)
	tx.assertRowsCount(20)
	tx.assertRowDoesNotExist(20)
	tx.assertRowExists(0)
	tx.assertRowExists(19)
	db.t.FatalOnError(tx.Rollback())
	db.sync()
	table.assertRowsCount(0)
}

func TestArguments(t *testing.T) {
	db := getRemoteDb(T{t})
	testArguments(db)
}

func TestArgumentsEmbedded(t *testing.T) {
	db := getEmbeddedDb(T{t})
	testArguments(db)
}

func testArguments(db *Database) {
	if db == nil {
		return
	}
	t := db.t
	tableName := fmt.Sprintf("test_%d", time.Now().UnixNano())
	_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, name TEXT, gpa REAL, cv BLOB);", tableName))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (id, name, gpa, cv) VALUES (?, ?, ?, randomblob(10));", tableName), 0, fmt.Sprint(0), 0.5)
	if err != nil {
		t.Fatal(err)
	}
	db.sync()
	rows, err := db.QueryContext(context.Background(), "SELECT NULL, id, name, gpa, cv FROM "+tableName)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	idx := 0
	for rows.Next() {
		if idx > 0 {
			t.Fatal("idx should be <= ", 0)
		}
		var null any
		var id int
		var name string
		var gpa float64
		var cv []byte
		if err := rows.Scan(&null, &id, &name, &gpa, &cv); err != nil {
			t.Fatal(err)
		}
		if null != nil {
			t.Fatal("null should be nil")
		}
		if id != int(idx) {
			t.Fatal("id should be ", idx, " got ", id)
		}
		if name != fmt.Sprint(idx) {
			t.Fatal("name should be", idx)
		}
		if gpa != float64(idx)+0.5 {
			t.Fatal("gpa should be", float64(idx)+0.5)
		}
		if len(cv) != 10 {
			t.Fatal("cv should be 10 bytes")
		}
		idx++
	}
	if idx != 1 {
		t.Fatal("idx should be 1 got ", idx)
	}
}

func TestPing(t *testing.T) {
	db := getRemoteDb(T{t})
	testPing(db)
}

func TestPingEmbedded(t *testing.T) {
	db := getEmbeddedDb(T{t})
	testPing(db)
}

func testPing(db *Database) {
	if db == nil {
		return
	}
	// This ping should succeed because the database is up and running
	db.t.FatalOnError(db.Ping())

	db.t.Cleanup(func() {
		db.Close()

		// This ping should return an error because the database is already closed
		err := db.Ping()
		if err == nil {
			db.t.Fatal("db.Ping succeeded when it should have failed")
		}
	})
}

func TestDataTypes(t *testing.T) {
	db := getRemoteDb(T{t})
	testDataTypes(db)
}

func TestDataTypesEmbedded(t *testing.T) {
	db := getEmbeddedDb(T{t})
	testDataTypes(db)
}

func testDataTypes(db *Database) {
	if db == nil {
		return
	}
	var (
		text        string
		nullText    sql.NullString
		integer     sql.NullInt64
		nullInteger sql.NullInt64
		boolean     bool
		float8      float64
		nullFloat   sql.NullFloat64
		bytea       []byte
		Time        time.Time
	)
	t := db.t
	db.t.FatalOnError(db.QueryRowContext(db.ctx, "SELECT 'foobar' as text, NULL as text,  NULL as integer, 42 as integer, 1 as boolean, X'000102' as bytea, 3.14 as float8, NULL as float8, '0001-01-01 01:00:00+00:00' as time;").Scan(&text, &nullText, &nullInteger, &integer, &boolean, &bytea, &float8, &nullFloat, &Time))
	switch {
	case text != "foobar":
		t.Error("value mismatch - text")
	case nullText.Valid:
		t.Error("null text is valid")
	case nullInteger.Valid:
		t.Error("null integer is valid")
	case !integer.Valid:
		t.Error("integer is not valid")
	case integer.Int64 != 42:
		t.Error("value mismatch - integer")
	case !boolean:
		t.Error("value mismatch - boolean")
	case float8 != 3.14:
		t.Error("value mismatch - float8")
	case !bytes.Equal(bytea, []byte{0, 1, 2}):
		t.Error("value mismatch - bytea")
	case nullFloat.Valid:
		t.Error("null float is valid")
	case !Time.Equal(time.Time{}.Add(time.Hour)):
		t.Error("value mismatch - time")
	}
}

func TestConcurrentOnSingleConnection(t *testing.T) {
	db := getRemoteDb(T{t})
	testConcurrentOnSingleConnection(db)
}

func TestConcurrentOnSingleConnectionEmbedded(t *testing.T) {
	db := getEmbeddedDb(T{t})
	testConcurrentOnSingleConnection(db)
}

func testConcurrentOnSingleConnection(db *Database) {
	if db == nil {
		return
	}
	t1 := db.createTable()
	t2 := db.createTable()
	t3 := db.createTable()
	t1.insertRowsInternal(1, 10, func(i int) sql.Result {
		return t1.db.exec("INSERT INTO "+t1.name+" VALUES(?, ?)", i, i)
	})
	t2.insertRowsInternal(1, 10, func(i int) sql.Result {
		return t2.db.exec("INSERT INTO "+t2.name+" VALUES(?, ?)", i, -1*i)
	})
	t3.insertRowsInternal(1, 10, func(i int) sql.Result {
		return t3.db.exec("INSERT INTO "+t3.name+" VALUES(?, ?)", i, 0)
	})
	db.sync()
	g, ctx := errgroup.WithContext(context.Background())
	conn, err := db.Conn(context.Background())
	db.t.FatalOnError(err)
	defer conn.Close()
	worker := func(t Table, check func(int) error) func() error {
		return func() error {
			for i := 1; i < 100; i++ {
				// Each iteration is wrapped into a function to make sure that `defer rows.Close()`
				// is called after each iteration not at the end of the outer function
				err := func() error {
					rows, err := conn.QueryContext(ctx, "SELECT b FROM "+t.name)
					if err != nil {
						return fmt.Errorf("%w: %s", err, string(debug.Stack()))
					}
					defer rows.Close()
					for rows.Next() {
						var v int
						err := rows.Scan(&v)
						if err != nil {
							return fmt.Errorf("%w: %s", err, string(debug.Stack()))
						}
						if err := check(v); err != nil {
							return fmt.Errorf("%w: %s", err, string(debug.Stack()))
						}
					}
					err = rows.Err()
					if err != nil {
						return fmt.Errorf("%w: %s", err, string(debug.Stack()))
					}
					return nil
				}()
				if err != nil {
					return err
				}
			}
			return nil
		}
	}
	g.Go(worker(t1, func(v int) error {
		if v <= 0 {
			return fmt.Errorf("got non-positive value from table1: %d", v)
		}
		return nil
	}))
	g.Go(worker(t2, func(v int) error {
		if v >= 0 {
			return fmt.Errorf("got non-negative value from table2: %d", v)
		}
		return nil
	}))
	g.Go(worker(t3, func(v int) error {
		if v != 0 {
			return fmt.Errorf("got non-zero value from table3: %d", v)
		}
		return nil
	}))
	db.t.FatalOnError(g.Wait())
}

func runFileTest(t *testing.T, test func(*testing.T, *sql.DB)) {
	dir, err := os.MkdirTemp("", "libsql-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	db, err := sql.Open("libsql", "file:"+dir+"/test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	test(t, db)
}

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
		runFileTest(t, test)
	})
}

func TestErrorNonUtf8URL(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("libsql", "file:a\xc5z")
	if err == nil {
		defer func() {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}()
		t.Fatal("expected error")
	}
	if err.Error() != "failed to open local database file:a\xc5z\nerror code = 1: Wrong URL: invalid utf-8 sequence of 1 bytes from index 6" {
		t.Fatal("unexpected error:", err)
	}
}

func TestErrorWrongURL(t *testing.T) {
	t.Skip("Does not work with v2")
	t.Parallel()
	db, err := sql.Open("libsql", "http://example.com/test")
	if err == nil {
		defer func() {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}()
		t.Fatal("expected error")
	}
	if err.Error() != "failed to open database http://example.com/test\nerror code = 1: Error opening URL http://example.com/test: Failed to connect to database: `Unable to open remote database http://example.com/test with Database::open()`" {
		t.Fatal("unexpected error:", err)
	}
}

func TestErrorCanNotConnect(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("libsql", "file:/root/test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	conn, err := db.Conn(context.Background())
	if err == nil {
		defer func() {
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
		}()
		t.Fatal("expected error")
	}
	if err.Error() != "failed to connect to database\nerror code = 1: Unable to connect: Failed to connect to database: `file:/root/test.db`" {
		t.Fatal("unexpected error:", err)
	}
}

func TestExec(t *testing.T) {
	runMemoryAndFileTests(t, func(t *testing.T, db *sql.DB) {
		if _, err := db.ExecContext(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT)"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestExecWithQuery(t *testing.T) {
	runMemoryAndFileTests(t, func(t *testing.T, db *sql.DB) {
		if _, err := db.QueryContext(context.Background(), "SELECT 1"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestErrorExec(t *testing.T) {
	runMemoryAndFileTests(t, func(t *testing.T, db *sql.DB) {
		_, err := db.ExecContext(context.Background(), "CREATE TABLES test (id INTEGER, name TEXT)")
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != "failed to execute query CREATE TABLES test (id INTEGER, name TEXT)\nerror code = 2: Error executing statement: SQLite failure: `near \"TABLES\": syntax error`" {
			t.Fatal("unexpected error:", err)
		}
	})
}

func TestQuery(t *testing.T) {
	runMemoryAndFileTests(t, func(t *testing.T, db *sql.DB) {
		if _, err := db.ExecContext(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT, gpa REAL, cv BLOB)"); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 10; i++ {
			if _, err := db.ExecContext(context.Background(), "INSERT INTO test VALUES(?, ?, ?, randomblob(10))", i, fmt.Sprint(i), float64(i)+0.5); err != nil {
				t.Fatal(err)
			}
		}
		rows, err := db.QueryContext(context.Background(), "SELECT NULL, id, name, gpa, cv FROM test")
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		assert.DeepEqual(t, columns, []string{"NULL", "id", "name", "gpa", "cv"})
		types, err := rows.ColumnTypes()
		if err != nil {
			t.Fatal(err)
		}
		if len(types) != 5 {
			t.Fatal("types should be 5")
		}
		defer rows.Close()
		idx := 0
		for rows.Next() {
			var null any
			var id int
			var name string
			var gpa float64
			var cv []byte
			if err := rows.Scan(&null, &id, &name, &gpa, &cv); err != nil {
				t.Fatal(err)
			}
			if null != nil {
				t.Fatal("null should be nil")
			}
			if id != int(idx) {
				t.Fatal("id should be", idx)
			}
			if name != fmt.Sprint(idx) {
				t.Fatal("name should be", idx)
			}
			if gpa != float64(idx)+0.5 {
				t.Fatal("gpa should be", float64(idx)+0.5)
			}
			if len(cv) != 10 {
				t.Fatal("cv should be 10 bytes")
			}
			idx++
		}
	})
}

func TestErrorQuery(t *testing.T) {
	runMemoryAndFileTests(t, func(t *testing.T, db *sql.DB) {
		rows, err := db.QueryContext(context.Background(), "SELECT NULL, id, name, gpa, cv FROM test")
		if rows != nil {
			rows.Close()
		}
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != "failed to execute query SELECT NULL, id, name, gpa, cv FROM test\nerror code = 1: Error executing statement: SQLite failure: `no such table: test`" {
			t.Fatal("unexpected error:", err)
		}
	})
}

func TestQueryWithEmptyResult(t *testing.T) {
	runMemoryAndFileTests(t, func(t *testing.T, db *sql.DB) {
		if _, err := db.ExecContext(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT, gpa REAL, cv BLOB)"); err != nil {
			t.Fatal(err)
		}
		rows, err := db.QueryContext(context.Background(), "SELECT NULL, id, name, gpa, cv FROM test")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		assert.DeepEqual(t, columns, []string{"NULL", "id", "name", "gpa", "cv"})
		types, err := rows.ColumnTypes()
		if err != nil {
			t.Fatal(err)
		}
		if len(types) != 5 {
			t.Fatal("types should be 5")
		}
		for rows.Next() {
			t.Fatal("there should be no rows")
		}
	})
}

func TestErrorRowsNext(t *testing.T) {
	runFileTest(t, func(t *testing.T, db *sql.DB) {
		db.Exec("PRAGMA journal_mode=DELETE")
		if _, err := db.ExecContext(context.Background(), "CREATE TABLE test (id INTEGER)"); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 10; i++ {
			if _, err := db.ExecContext(context.Background(), "INSERT INTO test VALUES("+fmt.Sprint(i)+")"); err != nil {
				t.Fatal(err)
			}
		}
		c1, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer c1.Close()
		c1.ExecContext(context.Background(), "PRAGMA journal_mode=DELETE")
		c2, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer c2.Close()
		c2.ExecContext(context.Background(), "PRAGMA journal_mode=DELETE")
		_, err = c1.ExecContext(context.Background(), "BEGIN EXCLUSIVE TRANSACTION")
		if err != nil {
			t.Fatal(err)
		}
		rows, err := c2.QueryContext(context.Background(), "SELECT id FROM test")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		if rows.Next() {
			t.Fatal("there should be no rows")
		}
		err = rows.Err()
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != "failed to get next row\nerror code = 1: Error fetching next row: SQLite failure: `database is locked`" {
			t.Fatal("unexpected error:", err)
		}
	})
}
