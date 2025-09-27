#include "libsql.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char *argv[])
{
	libsql_connection_t conn = NULL;
	libsql_rows_t rows = NULL;
	libsql_row_t row = NULL;
	libsql_database_t db = NULL;
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
	        snprintf(auth_token, sizeof(auth_token), "%s", argv[2]);
	    }
	    snprintf(db_path, sizeof(db_path), "%s", "test.db");
	    memset(&config, 0, sizeof(config));
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

    // --- ROLLBACK should discard changes
    {
        libsql_tx_t tx = NULL;
        retval = libsql_tx_begin(conn, 0 /* Deferred */, &tx, &err);
        if (retval != 0) {
            fprintf(stderr, "tx_begin (rollback test): %s\n", err);
            goto quit;
        }

        retval = libsql_execute(conn, "DELETE FROM guest_book_entries", &err);
        if (retval != 0) {
            fprintf(stderr, "delete before rollback test: %s\n", err);
            libsql_tx_free(tx);
            goto quit;
        }

        retval = libsql_execute(conn, "INSERT INTO guest_book_entries VALUES('tx will be rolled back')", &err);
        if (retval != 0) {
            fprintf(stderr, "insert (rollback test): %s\n", err);
            libsql_tx_free(tx);
            goto quit;
        }

        retval = libsql_tx_rollback(tx, &err);
        if (retval != 0) {
            fprintf(stderr, "tx_rollback: %s\n", err);
            libsql_tx_free(tx);
            goto quit;
        }
        tx = NULL;

        rows = NULL; row = NULL; err = NULL;
        retval = libsql_query(conn, "SELECT COUNT(*) FROM guest_book_entries", &rows, &err);
        if (retval != 0) {
            fprintf(stderr, "query count after rollback: %s\n", err);
            goto quit;
        }
        retval = libsql_next_row(rows, &row, &err);
        if (retval != 0 || !row) {
            fprintf(stderr, "next_row (count after rollback): %s\n", err ? err : "no row");
            goto quit;
        }
        long long count = -1;
        retval = libsql_get_int(row, 0, &count, &err);
        if (retval != 0) {
            fprintf(stderr, "get_int (count after rollback): %s\n", err);
            goto quit;
        }
        libsql_free_row(row); row = NULL;
        libsql_free_rows(rows); rows = NULL;

        if (count != 0) {
            fprintf(stderr, "rollback test failed: expected 0 rows, got %lld\n", count);
            retval = 1;
            goto quit;
        } else {
            printf("[tx-rollback] OK: count=%lld\n", count);
        }
    }

    // --- COMMIT should persist changes
    {
        libsql_tx_t tx = NULL;
        retval = libsql_tx_begin(conn, 0 /* Deferred */, &tx, &err);
        if (retval != 0) {
            fprintf(stderr, "tx_begin (commit test): %s\n", err);
            goto quit;
        }

        retval = libsql_execute(conn, "INSERT INTO guest_book_entries VALUES('hello from tx-commit 1')", &err);
        if (retval != 0) {
            fprintf(stderr, "insert 1 (commit test): %s\n", err);
            libsql_tx_free(tx);
            goto quit;
        }
        retval = libsql_execute(conn, "INSERT INTO guest_book_entries VALUES('hello from tx-commit 2')", &err);
        if (retval != 0) {
            fprintf(stderr, "insert 2 (commit test): %s\n", err);
            libsql_tx_free(tx);
            goto quit;
        }

        retval = libsql_tx_commit(tx, &err);
        if (retval != 0) {
            fprintf(stderr, "tx_commit: %s\n", err);
            libsql_tx_free(tx);
            goto quit;
        }

        tx = NULL;
        rows = NULL; row = NULL; err = NULL;
        retval = libsql_query(conn, "SELECT COUNT(*) FROM guest_book_entries", &rows, &err);
        if (retval != 0) {
            fprintf(stderr, "query count after commit: %s\n", err);
            goto quit;
        }
        retval = libsql_next_row(rows, &row, &err);
        if (retval != 0 || !row) {
            fprintf(stderr, "next_row (count after commit): %s\n", err ? err : "no row");
            goto quit;
        }
        long long count = -1;
        retval = libsql_get_int(row, 0, &count, &err);
        if (retval != 0) {
            fprintf(stderr, "get_int (count after commit): %s\n", err);
            goto quit;
        }
        libsql_free_row(row); row = NULL;
        libsql_free_rows(rows); rows = NULL;

        if (count != 2) {
            fprintf(stderr, "commit test failed: expected 2 rows, got %lld\n", count);
            retval = 1;
            goto quit;
        } else {
            printf("[tx-commit] OK: count=%lld\n", count);
        }
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

        libsql_free_row(row);
        row = NULL;
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
	if (row) libsql_free_row(row);
	if (rows) libsql_free_rows(rows);
	if (conn) libsql_disconnect(conn);
	if (db) libsql_close(db);

	return retval;
}
