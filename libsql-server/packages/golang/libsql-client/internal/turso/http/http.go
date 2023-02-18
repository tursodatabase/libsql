package tursohttp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/xwb1989/sqlparser"
)

type Params struct {
	Names  []string
	Values []any
}

func (p *Params) MarshalJSON() ([]byte, error) {
	if len(p.Values) == 0 {
		return json.Marshal([]string{})
	}
	if len(p.Names) == 0 {
		return json.Marshal(p.Values)
	}
	m := map[string]interface{}{}
	for idx := range p.Values {
		m["@"+p.Names[idx]] = p.Values[idx]
	}
	return json.Marshal(m)
}

type ResultSet struct {
	Columns []string `json:"columns"`
	Rows    []Row    `json:"rows"`
}

type Row []interface{}

func CallTurso(url string, sql string, params Params) (*ResultSet, error) {
	stmts, err := sqlparser.SplitStatementToPieces(sql)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("wrong number of statements in SQL: %s\nexpected 1 got %d", sql, len(stmts))
	}

	type Statement struct {
		Query  string `json:"q"`
		Params Params `json:"params"`
	}

	rawReq := struct {
		Statements []Statement `json:"statements"`
	}{
		Statements: []Statement{{sql, params}},
	}

	req, err := json.Marshal(rawReq)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(req))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		var err_response struct {
			Message string `json:"error"`
		}
		if err := json.Unmarshal(body, &err_response); err != nil {
			return nil, fmt.Errorf("failed to execute SQL: %s", sql)
		}
		return nil, fmt.Errorf("failed to execute SQL: %s\n%s", sql, err_response.Message)
	}

	type errObject struct {
		Message string `json:"message"`
	}

	var results []struct {
		Results *ResultSet `json:"results"`
		Error   *errObject `json:"error"`
	}
	if err := json.Unmarshal(body, &results); err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, fmt.Errorf("wrong number of results for SQL: %s\nexpected 1 got %d", sql, len(results))
	}
	if results[0].Error != nil {
		return nil, fmt.Errorf("failed to execute SQL: %s\n%s", sql, results[0].Error.Message)
	}
	if results[0].Results == nil {
		return nil, fmt.Errorf("no results for SQL: %s", sql)
	}
	return results[0].Results, nil
}
