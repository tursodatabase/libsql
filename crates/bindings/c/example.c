/* gcc -I include example.c ../../target/debug/libsql_experimental.a && ./a.out */

#include "libsql.h"
#include <assert.h>

int main(int argc, char *argv[])
{
	libsql_database_ref database;

	database = libsql_open(":memory:");
	if (!database) {
		assert(0);
	}

	libsql_exec(database, "SELECT 1");

	libsql_close(database);
}
