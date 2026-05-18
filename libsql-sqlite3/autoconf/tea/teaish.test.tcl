test-expect 1.0-open {
  sqlite3 db :memory:
} {}

test-assert 1.1-version-3.x {
  [string match 3.* [db eval {select sqlite_version()}]]
}

test-expect 1.2-select {
  db eval {select 'hi, world',1,2,3}
} {{hi, world} 1 2 3}


test-expect 99.0-db-close {db close} {}
