#include <sqlite3mc/sqlite3mc.h>
#include <stdio.h>

int main(int argc, char *argv[])
{
    sqlite3 *db;
    const char *key = "password";
    const char *wrongKey = "wrongPassword";

    // Create database
    int rc = sqlite3_open("test.db", &db);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "Failed to open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    // Encrypt
    sqlite3_key(db, key, strlen(key));

    // Fill db with some data and close it
    rc = sqlite3_exec(db, "CREATE TABLE users (name TEXT NOT NULL, ID INTEGER PRIMARY KEY UNIQUE)", NULL, NULL, NULL);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }
    rc = sqlite3_exec(db, "INSERT INTO users (name, ID) VALUES ('testUser', '12345')", NULL, NULL, NULL);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }
    rc = sqlite3_close(db);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "Failed to close database: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    // Reopen and provide wrong key
    rc = sqlite3_open("test.db", &db);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "Failed to open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }
    sqlite3_key(db, wrongKey, strlen(wrongKey));

    // Try to access the database, should fail
    rc = sqlite3_exec(db, "SELECT name FROM users WHERE ID = '12345'", NULL, NULL, NULL);
    if (rc == SQLITE_OK)
    {
        fprintf(stderr, "Access was provided without the proper key\n");
        sqlite3_close(db);
        return 1;
    }
    rc = sqlite3_close(db);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "Failed to close database: %s\n", sqlite3_errmsg(db));
        return 1;
    }


    fprintf(stdout, "Test successful\n");
    return 0;
}
