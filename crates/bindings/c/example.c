/* gcc -I include example.c ../../target/debug/libsql_experimental.a ../../../.libs/libsqlite3.a && ./a.out */

#include "libsql.h"
#include <assert.h>
#include <stdio.h>

int main(int argc, char *argv[])
{
	libsql_connection_t conn;
	libsql_result_t result;
	libsql_database_t db;

	db = libsql_open_ext("libsql://penberg.turso.io");
	if (!db) {
		assert(0);
	}
	conn = libsql_connect(db);
	if (!conn) {
		assert(0);
	}
	result = libsql_execute(conn, "SELECT 1");
	if (!result) {
		assert(0);
	}
	libsql_wait_result(result);
	for (int row = 0; row < libsql_row_count(result); row++) {
		for (int col = 0; col < libsql_column_count(result); col++) {
			if (col > 0) {
				printf(", ");
			}
			const char *value = libsql_value_text(result, row, col);
			printf("%s", value);
		}
	}
	libsql_free_result(result);
	libsql_disconnect(conn);
	libsql_close(db);
}
