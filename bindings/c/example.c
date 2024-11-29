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
	char sync = 0;

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
        sync = 1;
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

	retval = libsql_execute(conn, "CREATE TABLE IF NOT EXISTS guest_book_entries (text TEXT)", &err);
	if (retval != 0) {
        fprintf(stderr, "%s\n", err);
        goto quit;
    }

	retval = libsql_execute(conn, "INSERT INTO guest_book_entries VALUES('hi there')", &err);
	if (retval != 0) {
        fprintf(stderr, "%s\n", err);
        goto quit;
    }

	retval = libsql_execute(conn, "INSERT INTO guest_book_entries VALUES('some more hi there')", &err);
	if (retval != 0) {
        fprintf(stderr, "%s\n", err);
        goto quit;
    }

	retval = libsql_query(conn, "SELECT text FROM guest_book_entries", &rows, &err);
	if (retval != 0) {
		fprintf(stderr, "%s\n", err);
		goto quit;
	}

	while ((retval = libsql_next_row(rows, &row, &err)) == 0) {
		if (!err && !row) {
			break;
		}
        const char * value = NULL;
        retval = libsql_get_string(row, 0, &value, &err);
        if (retval != 0) {
            fprintf(stderr, "%s\n", err);
        } else {
            printf("%s\n", value);
            libsql_free_string(value);
            value = NULL;
        }
		err = NULL;
	}

	if (retval != 0) {
		fprintf(stderr, "%s\n", err);
		goto quit;
	}

    if (sync) {
        printf("Syncing database to remote...\n");
        retval = libsql_sync(db, &err);
        if (retval != 0) {
            fprintf(stderr, "%s\n", err);
            goto quit;
        }
        printf("Done!\n");
	}

quit:
	libsql_free_rows(rows);
	libsql_disconnect(conn);
	libsql_close(db);

	return retval;
}
