#include "libsql.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char *argv[])
{
	libsql_connection_t conn;
	libsql_rows_t rows;
	libsql_row_t row;
	libsql_database_t db;
	libsql_config config;
	const char *err = NULL;
	int retval = 0;
	char db_path[1024];

	if (argc > 1) {
	    char* url = argv[1];
	    char auth_token[1024];
	    auth_token[0] = '\0';
	    if (argc > 2) {
	        strncpy(auth_token, argv[2], strlen(argv[2]));
	    }
	    strncpy(db_path, "test.db", strlen("test.db"));
	    config.db_path = db_path;
	    config.primary_url = url;
	    config.auth_token = auth_token;
	    config.read_your_writes = 0;
	    config.encryption_key = NULL;
	    config.sync_interval = 0;
	    config.with_webpki = 0;
	    retval = libsql_open_sync_with_config(config, &db, &err);
        if (retval != 0) {
            fprintf(stderr, "%s\n", err);
            goto quit;
        }
    } else {
        retval = libsql_open_ext(":memory:", &db, &err);
        if (retval != 0) {
            fprintf(stderr, "%s\n", err);
            goto quit;
        }
    }

	retval = libsql_connect(db, &conn, &err);
	if (retval != 0) {
		fprintf(stderr, "%s\n", err);
		goto quit;
	}

	retval = libsql_query(conn, "SELECT 1", &rows, &err);
	if (retval != 0) {
		fprintf(stderr, "%s\n", err);
		goto quit;
	}

	int num_cols = libsql_column_count(rows);

	while ((retval = libsql_next_row(rows, &row, &err)) == 0) {
		if (!err && !row) {
			break;
		}
		for (int col = 0; col < num_cols; col++) {
			if (col > 0) {
				printf(", ");
			}
			long long value;
			retval = libsql_get_int(row, col, &value, &err);
			if (retval != 0) {
				fprintf(stderr, "%s\n", err);
			} else {
				printf("%lld\n", value);
			}
		}
		err = NULL;
	}

	if (retval != 0) {
		fprintf(stderr, "%s\n", err);
		goto quit;
	}

quit:
	libsql_free_rows(rows);
	libsql_disconnect(conn);
	libsql_close(db);

	return retval;
}
