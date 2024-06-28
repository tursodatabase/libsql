#include "../parse.h"
#include "../opcodes.h"
#include "../src/sqliteInt.h"
#include "../src/vectorInt.h"
#include "../src/vdbeInt.h"
#include "assert.h"
#include "stdbool.h"
#include "ctype.h"
#include "string.h"
#include "stdarg.h"
#include "time.h"
#include "sys/stat.h"

#define eprintf(...) fprintf(stderr, __VA_ARGS__)
#define ensure(condition, ...) { if (!(condition)) { eprintf(__VA_ARGS__); exit(1); } }

int create_query_template(char* query, char* template, char** parameters, int* lengths, int* types) {
  bool in_quote = false; 
  bool in_digit = false;
  int parameter = 0;
  char* parameter_start = 0;

  if (strncmp(query, "INSERT", 6) != 0 && strncmp(query, "SELECT", 6) != 0) {
    return 0;
  }
  for (; *query != '\0'; query++) {
    if (*query == '\'' && (*(query + 1) == '[' || *(query - 1) == ']')) {
      if (!in_quote) {
        parameter_start = query;
      } else {
        *(template++) = '?';
        parameters[parameter] = parameter_start + 1;
        lengths[parameter] = query - parameter_start - 1;
        types[parameter] = 1;
        parameter++;
      }
      in_quote = !in_quote;
      continue;
    }
    if (in_quote) {
      continue;
    }
    if (isdigit(*query)) {
      if (!in_digit) {
        parameter_start = query;
      }
      in_digit = true;
      continue;
    }
    if (in_digit) {
      *(template++) = '?';
      parameters[parameter] = parameter_start;
      lengths[parameter] = query - parameter_start + 1;
      types[parameter] = 0;
      parameter++;
    }
    in_digit = false;
    *(template++) = *query;
  }
  *(template++) = '\0';
  return parameter;
}

int get_int(char* s, int length) {
  char buffer[32];
  strncpy(buffer, s, length + 1);
  return atoi(buffer); 
}

int main(int argc, char* argv[]) {
  ensure(argc == 3, "provide path to the query file and db file\n");
  sqlite3* db;
  int rc = sqlite3_open(argv[2], &db);
  ensure(rc == 0, "failed to open db: rc=%d\n", rc);
  eprintf("open in-memory sqlite db\n");
  
  FILE* queries_f = fopen(argv[1], "r");
  ensure(queries_f != NULL, "failed to open queries file\n");
  eprintf("open queries file at %s\n", argv[1]);

  char line[65536 * 32];
  char template[65536 * 32];
  char* parameters[16];
  int parameter_lengths[16];
  int parameter_types[16];

  sqlite3_stmt* statement;
  char prepared[65536 * 32];

  double total_select_time = 0;
  double total_insert_time = 0;
  double total_delete_time = 0;
  int total_selects = 0;
  int total_inserts = 0;
  int total_deletes = 0;
  while (fgets(line, sizeof(line), queries_f)) {
    int len = strlen(line);
    char* end = line + len - 1;
    while (*end == '\n' || *end == '\r' || *end == ' ') {
      *(end--) = '\0';
    }

    if (strncmp(line, "---", 3) == 0) {
      rc = sqlite3_wal_checkpoint_v2(db, 0, SQLITE_CHECKPOINT_FULL, 0, 0);
      ensure(rc == 0, "failed to checkpoint db: %s\n", sqlite3_errmsg(db));
      // print & reset stat
      printf("%s (%s):\n", line + 3, argv[1]);
      if (total_selects > 0) {
        printf("  select: %.2f micros (avg.), %d (count)\n", total_select_time / total_selects * 1000000, total_selects);
      }
      if (total_inserts > 0) {
        printf("  insert: %.2f micros (avg.), %d (count)\n", total_insert_time / total_inserts * 1000000, total_inserts);
      }
      if (total_deletes > 0) {
        printf("  delete: %.2f micros (avg.), %d (count)\n", total_delete_time / total_deletes * 1000000, total_deletes);
      }
      struct stat st;
      stat(argv[2], &st);
      printf("  size  : %.4f MB\n", st.st_size / 1024.0 / 1024.0);
      fflush(stdout);

      total_select_time = 0; 
      total_insert_time = 0; 
      total_delete_time = 0; 
      total_selects = 0; 
      total_inserts = 0; 
      total_deletes = 0; 
      continue;
    }

    char* error;
    int count = create_query_template(line, template, (char**)&parameters, parameter_lengths, parameter_types);
    if (count > 0) {
      if (strcmp(template, prepared) != 0) {
        if (statement) {
          sqlite3_finalize(statement);
        }
        // prepare statement
        memcpy(prepared, template, strlen(template) + 1);
        int rc = sqlite3_prepare_v2(db, template, strlen(template), &statement, 0);
        ensure(rc == 0, "failed to prepare statement '%s': %d\n", template, rc);
        eprintf("prepared statement: '%s'\n", template);
      }

      rc = sqlite3_reset(statement);
      ensure(rc == 0, "failed to reset prepared statement: %s", sqlite3_errmsg(db));
      rc = sqlite3_clear_bindings(statement);
      ensure(rc == 0, "failed to clear bindings for prepared statement: %s", sqlite3_errmsg(db));

      // bind parameters
      int i = 0;
      for (; i < count; i++) {
        if (parameter_types[i] == 0) {
          rc = sqlite3_bind_int(statement, i + 1, get_int(parameters[i], parameter_lengths[i]));
          ensure(rc == 0, "failed to bind int parameter (%d): %s\n", i, sqlite3_errmsg(db));
        }
        if (parameter_types[i] == 1) {
          rc = sqlite3_bind_text(statement, i + 1, parameters[i], parameter_lengths[i], 0);
          ensure(rc == 0, "failed to bind string parameter: %d\n", rc);
        }
      }
      time_t start_time, end_time;
      start_time = clock();
      double* total_time = 0;
      int* total_count = 0;
      if (strncmp(prepared, "SELECT", 6) == 0) {
        total_time = &total_select_time;
        total_count = &total_selects;

        do {
          rc = sqlite3_step(statement); 
        } while (rc == SQLITE_ROW);
        ensure(rc == SQLITE_DONE, "SELECT query finished incorrectly: %s", sqlite3_errmsg(db));
      } else if (strncmp(prepared, "INSERT", 6) == 0) {
        total_time = &total_insert_time;
        total_count = &total_inserts;

        rc = sqlite3_step(statement);
        ensure(rc == SQLITE_DONE, "INSERT/DELETE query finished incorrectly: %s", sqlite3_errmsg(db));
      } else if (strncmp(prepared, "INSERT", 6) == 0) {
        total_time = &total_delete_time;
        total_count = &total_deletes;

        rc = sqlite3_step(statement);
        ensure(rc == SQLITE_DONE, "INSERT/DELETE query finished incorrectly: %s", sqlite3_errmsg(db));
      } else {
        ensure(false, "unexpected query type: %s\n", prepared);
      }
      end_time = clock();
      *total_time += (end_time - start_time) * 1.0 / CLOCKS_PER_SEC;
      *total_count += 1;
    } else {
      int rc = sqlite3_exec(db, line, 0, 0, &error);
      ensure(rc == 0, "failed to exec simple statement '%s': %s\n", line, error);
      eprintf("executed simple statement: '%s'\n", line);
    }
  }

  sqlite3_close(db);
  return 0;
}
