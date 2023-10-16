/* gcc -I include example.c ../../target/debug/libsql_experimental.a ../../../.libs/libsqlite3.a && ./a.out */

#include "libsql.h"
#include <assert.h>
#include <stdio.h>

int main(int argc, char *argv[])
{
	libsql_connection_t conn;
	libsql_rows_t rows;
	libsql_database_t db;

	db = libsql_open_ext(":memory:");
	if (!db) {
		assert(0);
	}
	conn = libsql_connect(db);
	if (!conn) {
		assert(0);
	}
	rows = libsql_execute(conn, "SELECT 1");
	if (!rows) {
		assert(0);
	}
	for (int row = 0; row < libsql_row_count(rows); row++) {
		for (int col = 0; col < libsql_column_count(rows); col++) {
			if (col > 0) {
				printf(", ");
			}
			const char *value = libsql_value_text(rows, row, col);
			printf("%s", value);
		}
	}
	libsql_free_rows(rows);
	libsql_disconnect(conn);
	libsql_close(db);
}
