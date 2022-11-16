#include <stdio.h>
#include <stdlib.h>

#define SQLITE_OK	0
#define SQLITE_ERROR	1
#define SQLITE_DONE	101

typedef struct sqlite3 {
} sqlite3;

typedef struct sqlite3_stmt {
} sqlite3_stmt;

static struct sqlite3 *sqlite3_new(void)
{
	return malloc(sizeof(struct sqlite3));
}

static void sqlite3_delete(struct sqlite3 *db)
{
	free(db);
}

static struct sqlite3_stmt *sqlite3_stmt_new(void)
{
	return malloc(sizeof(struct sqlite3_stmt));
}

static void sqlite3_stmt_delete(struct sqlite3_stmt *db)
{
	free(db);
}

#define STUB() printf("STUB %s\n", __func__)
#define TRACE() printf("TRACE %s\n", __func__)

#define DEFINE_STUB(func_name)				\
	int func_name(void)				\
	{						\
		printf("STUB %s\n", #func_name);	\
		return SQLITE_ERROR;			\
	}						\

/*
 * Library version numbers.
 */

#define SQLITE_VERSION        "3.39.3"
#define SQLITE_VERSION_NUMBER 3039003

const char *sqlite3_version = SQLITE_VERSION;

const char *sqlite3_libversion(void)
{
	return SQLITE_VERSION;
}

int sqlite3_libversion_number(void)
{
	return SQLITE_VERSION_NUMBER;
}

/*
 * Initialize the library.
 */

int sqlite3_initialize(void)
{
	STUB();
	return SQLITE_OK;
}

int sqlite3_shutdown(void)
{
	STUB();
	return SQLITE_OK;
}

int sqlite3_os_init(void)
{
	STUB();
	return SQLITE_OK;
}

int sqlite3_os_end(void)
{
	STUB();
	return SQLITE_OK;
}

/*
 * Error codes and messages.
 */

int sqlite3_errcode(sqlite3 *db)
{
	STUB();
	return SQLITE_OK;
}

int sqlite3_extended_errcode(sqlite3 *db)
{
	STUB();
	return SQLITE_OK;
}

const char *sqlite3_errmsg(sqlite3*)
{
	STUB();
	return "unknown error";
}

const void *sqlite3_errmsg16(sqlite3*)
{
	STUB();
	return NULL;
}

const char *sqlite3_errstr(int)
{
	STUB();
	return NULL;
}

int sqlite3_error_offset(sqlite3 *db)
{
	STUB();
	return SQLITE_OK;
}

/*
 * Opening a database connection.
 */

int sqlite3_open(const char *filename, sqlite3 **ppDb)
{
	TRACE();
	struct sqlite3 *db = sqlite3_new();
	*ppDb = db;
	return SQLITE_OK;
}

int sqlite3_open16(const void *filename, sqlite3 **ppDb)
{
	TRACE();
	struct sqlite3 *db = sqlite3_new();
	*ppDb = db;
	return SQLITE_OK;
}

int sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags, const char *zVfs)
{
	TRACE();
	struct sqlite3 *db = sqlite3_new();
	*ppDb = db;
	return SQLITE_OK;
}

/*
 * Closing a database connection.
 */

int sqlite3_close(sqlite3* pDb)
{
	TRACE();
	sqlite3_delete(pDb);
	return SQLITE_OK;
}

int sqlite3_close_v2(sqlite3* pDb)
{
	TRACE();
	sqlite3_delete(pDb);
	return SQLITE_OK;
}

/*
 * Prepared statements.
 */

int sqlite3_prepare_v2(sqlite3 *db, const char *zSql, int nByte, sqlite3_stmt **ppStmt, const char **pzTail)
{
	TRACE();
	struct sqlite3_stmt *stmt = sqlite3_stmt_new();
	*ppStmt = stmt;
	*pzTail = "";
	return SQLITE_OK;
}

int sqlite3_finalize(sqlite3_stmt *pStmt)
{
	TRACE();
	sqlite3_stmt_delete(pStmt);
	return SQLITE_OK;
}

/*
 * SQL evaluation.
 */

int sqlite3_step(sqlite3_stmt* pStmt)
{
	TRACE();
	return SQLITE_DONE;
}

/*
 * Stubs.
 */

DEFINE_STUB(sqlite3_aggregate_context);
DEFINE_STUB(sqlite3_aggregate_count);
DEFINE_STUB(sqlite3_auto_extension);
DEFINE_STUB(sqlite3_autovacuum_pages);
DEFINE_STUB(sqlite3_backup_finish);
DEFINE_STUB(sqlite3_backup_init);
DEFINE_STUB(sqlite3_backup_pagecount);
DEFINE_STUB(sqlite3_backup_remaining);
DEFINE_STUB(sqlite3_backup_step);
DEFINE_STUB(sqlite3_bind_blob);
DEFINE_STUB(sqlite3_bind_blob64);
DEFINE_STUB(sqlite3_bind_double);
DEFINE_STUB(sqlite3_bind_int);
DEFINE_STUB(sqlite3_bind_int64);
DEFINE_STUB(sqlite3_bind_null);
DEFINE_STUB(sqlite3_bind_parameter_count);
DEFINE_STUB(sqlite3_bind_parameter_index);
DEFINE_STUB(sqlite3_bind_parameter_name);
DEFINE_STUB(sqlite3_bind_pointer);
DEFINE_STUB(sqlite3_bind_text);
DEFINE_STUB(sqlite3_bind_text16);
DEFINE_STUB(sqlite3_bind_text64);
DEFINE_STUB(sqlite3_bind_value);
DEFINE_STUB(sqlite3_bind_zeroblob);
DEFINE_STUB(sqlite3_bind_zeroblob64);
DEFINE_STUB(sqlite3_blob_bytes);
DEFINE_STUB(sqlite3_blob_close);
DEFINE_STUB(sqlite3_blob_open);
DEFINE_STUB(sqlite3_blob_read);
DEFINE_STUB(sqlite3_blob_reopen);
DEFINE_STUB(sqlite3_blob_write);
DEFINE_STUB(sqlite3_busy_handler);
DEFINE_STUB(sqlite3_busy_timeout);
DEFINE_STUB(sqlite3_cancel_auto_extension);
DEFINE_STUB(sqlite3_changes);
DEFINE_STUB(sqlite3_changes64);
DEFINE_STUB(sqlite3_clear_bindings);
DEFINE_STUB(sqlite3_collation_needed);
DEFINE_STUB(sqlite3_collation_needed16);
DEFINE_STUB(sqlite3_column_blob);
DEFINE_STUB(sqlite3_column_bytes);
DEFINE_STUB(sqlite3_column_bytes16);
DEFINE_STUB(sqlite3_column_count);
DEFINE_STUB(sqlite3_column_database_name);
DEFINE_STUB(sqlite3_column_database_name16);
DEFINE_STUB(sqlite3_column_decltype);
DEFINE_STUB(sqlite3_column_decltype16);
DEFINE_STUB(sqlite3_column_double);
DEFINE_STUB(sqlite3_column_int);
DEFINE_STUB(sqlite3_column_int64);
DEFINE_STUB(sqlite3_column_name);
DEFINE_STUB(sqlite3_column_name16);
DEFINE_STUB(sqlite3_column_origin_name);
DEFINE_STUB(sqlite3_column_origin_name16);
DEFINE_STUB(sqlite3_column_table_name);
DEFINE_STUB(sqlite3_column_table_name16);
DEFINE_STUB(sqlite3_column_text);
DEFINE_STUB(sqlite3_column_text16);
DEFINE_STUB(sqlite3_column_type);
DEFINE_STUB(sqlite3_column_value);
DEFINE_STUB(sqlite3_commit_hook);
DEFINE_STUB(sqlite3_compileoption_get);
DEFINE_STUB(sqlite3_compileoption_used);
DEFINE_STUB(sqlite3_complete);
DEFINE_STUB(sqlite3_complete16);
DEFINE_STUB(sqlite3_config);
DEFINE_STUB(sqlite3_context_db_handle);
DEFINE_STUB(sqlite3_create_collation);
DEFINE_STUB(sqlite3_create_collation16);
DEFINE_STUB(sqlite3_create_collation_v2);
DEFINE_STUB(sqlite3_create_filename);
DEFINE_STUB(sqlite3_create_function);
DEFINE_STUB(sqlite3_create_function16);
DEFINE_STUB(sqlite3_create_function_v2);
DEFINE_STUB(sqlite3_create_module);
DEFINE_STUB(sqlite3_create_module_v2);
DEFINE_STUB(sqlite3_create_window_function);
DEFINE_STUB(sqlite3_data_count);
DEFINE_STUB(sqlite3_database_file_object);
DEFINE_STUB(sqlite3_db_cacheflush);
DEFINE_STUB(sqlite3_db_config);
DEFINE_STUB(sqlite3_db_filename);
DEFINE_STUB(sqlite3_db_handle);
DEFINE_STUB(sqlite3_db_mutex);
DEFINE_STUB(sqlite3_db_name);
DEFINE_STUB(sqlite3_db_readonly);
DEFINE_STUB(sqlite3_db_release_memory);
DEFINE_STUB(sqlite3_db_status);
DEFINE_STUB(sqlite3_declare_vtab);
DEFINE_STUB(sqlite3_deserialize);
DEFINE_STUB(sqlite3_drop_modules);
DEFINE_STUB(sqlite3_enable_load_extension);
DEFINE_STUB(sqlite3_enable_shared_cache);
DEFINE_STUB(sqlite3_exec);
DEFINE_STUB(sqlite3_expanded_sql);
DEFINE_STUB(sqlite3_expired);
DEFINE_STUB(sqlite3_extended_result_codes);
DEFINE_STUB(sqlite3_file_control);
DEFINE_STUB(sqlite3_filename_database);
DEFINE_STUB(sqlite3_filename_journal);
DEFINE_STUB(sqlite3_filename_wal);
DEFINE_STUB(sqlite3_free);
DEFINE_STUB(sqlite3_free_filename);
DEFINE_STUB(sqlite3_free_table);
DEFINE_STUB(sqlite3_get_autocommit);
DEFINE_STUB(sqlite3_get_auxdata);
DEFINE_STUB(sqlite3_get_table);
DEFINE_STUB(sqlite3_global_recover);
DEFINE_STUB(sqlite3_hard_heap_limit64);
DEFINE_STUB(sqlite3_interrupt);
DEFINE_STUB(sqlite3_keyword_check);
DEFINE_STUB(sqlite3_keyword_count);
DEFINE_STUB(sqlite3_keyword_name);
DEFINE_STUB(sqlite3_last_insert_rowid);
DEFINE_STUB(sqlite3_limit);
DEFINE_STUB(sqlite3_load_extension);
DEFINE_STUB(sqlite3_log);
DEFINE_STUB(sqlite3_malloc);
DEFINE_STUB(sqlite3_malloc64);
DEFINE_STUB(sqlite3_memory_alarm);
DEFINE_STUB(sqlite3_memory_highwater);
DEFINE_STUB(sqlite3_memory_used);
DEFINE_STUB(sqlite3_mprintf);
DEFINE_STUB(sqlite3_msize);
DEFINE_STUB(sqlite3_mutex_alloc);
DEFINE_STUB(sqlite3_mutex_enter);
DEFINE_STUB(sqlite3_mutex_free);
DEFINE_STUB(sqlite3_mutex_held);
DEFINE_STUB(sqlite3_mutex_leave);
DEFINE_STUB(sqlite3_mutex_notheld);
DEFINE_STUB(sqlite3_mutex_try);
DEFINE_STUB(sqlite3_next_stmt);
DEFINE_STUB(sqlite3_normalized_sql);
DEFINE_STUB(sqlite3_overload_function);
DEFINE_STUB(sqlite3_prepare);
DEFINE_STUB(sqlite3_prepare16);
DEFINE_STUB(sqlite3_prepare16_v2);
DEFINE_STUB(sqlite3_prepare16_v3);
DEFINE_STUB(sqlite3_prepare_v3);
DEFINE_STUB(sqlite3_preupdate_blobwrite);
DEFINE_STUB(sqlite3_preupdate_count);
DEFINE_STUB(sqlite3_preupdate_depth);
DEFINE_STUB(sqlite3_preupdate_hook);
DEFINE_STUB(sqlite3_preupdate_new);
DEFINE_STUB(sqlite3_preupdate_old);
DEFINE_STUB(sqlite3_profile);
DEFINE_STUB(sqlite3_progress_handler);
DEFINE_STUB(sqlite3_randomness);
DEFINE_STUB(sqlite3_realloc);
DEFINE_STUB(sqlite3_realloc64);
DEFINE_STUB(sqlite3_release_memory);
DEFINE_STUB(sqlite3_reset);
DEFINE_STUB(sqlite3_reset_auto_extension);
DEFINE_STUB(sqlite3_result_blob);
DEFINE_STUB(sqlite3_result_blob64);
DEFINE_STUB(sqlite3_result_double);
DEFINE_STUB(sqlite3_result_error);
DEFINE_STUB(sqlite3_result_error16);
DEFINE_STUB(sqlite3_result_error_code);
DEFINE_STUB(sqlite3_result_error_nomem);
DEFINE_STUB(sqlite3_result_error_toobig);
DEFINE_STUB(sqlite3_result_int);
DEFINE_STUB(sqlite3_result_int64);
DEFINE_STUB(sqlite3_result_null);
DEFINE_STUB(sqlite3_result_pointer);
DEFINE_STUB(sqlite3_result_subtype);
DEFINE_STUB(sqlite3_result_text);
DEFINE_STUB(sqlite3_result_text16);
DEFINE_STUB(sqlite3_result_text16be);
DEFINE_STUB(sqlite3_result_text16le);
DEFINE_STUB(sqlite3_result_text64);
DEFINE_STUB(sqlite3_result_value);
DEFINE_STUB(sqlite3_result_zeroblob);
DEFINE_STUB(sqlite3_result_zeroblob64);
DEFINE_STUB(sqlite3_rollback_hook);
DEFINE_STUB(sqlite3_serialize);
DEFINE_STUB(sqlite3_set_authorizer);
DEFINE_STUB(sqlite3_set_auxdata);
DEFINE_STUB(sqlite3_set_last_insert_rowid);
DEFINE_STUB(sqlite3_sleep);
DEFINE_STUB(sqlite3_snapshot_cmp);
DEFINE_STUB(sqlite3_snapshot_free);
DEFINE_STUB(sqlite3_snapshot_get);
DEFINE_STUB(sqlite3_snapshot_open);
DEFINE_STUB(sqlite3_snapshot_recover);
DEFINE_STUB(sqlite3_snprintf);
DEFINE_STUB(sqlite3_soft_heap_limit);
DEFINE_STUB(sqlite3_soft_heap_limit64);
DEFINE_STUB(sqlite3_sourceid);
DEFINE_STUB(sqlite3_sql);
DEFINE_STUB(sqlite3_status);
DEFINE_STUB(sqlite3_status64);
DEFINE_STUB(sqlite3_stmt_busy);
DEFINE_STUB(sqlite3_stmt_isexplain);
DEFINE_STUB(sqlite3_stmt_readonly);
DEFINE_STUB(sqlite3_stmt_scanstatus);
DEFINE_STUB(sqlite3_stmt_scanstatus_reset);
DEFINE_STUB(sqlite3_stmt_status);
DEFINE_STUB(sqlite3_str_append);
DEFINE_STUB(sqlite3_str_appendall);
DEFINE_STUB(sqlite3_str_appendchar);
DEFINE_STUB(sqlite3_str_appendf);
DEFINE_STUB(sqlite3_str_errcode);
DEFINE_STUB(sqlite3_str_finish);
DEFINE_STUB(sqlite3_str_length);
DEFINE_STUB(sqlite3_str_new);
DEFINE_STUB(sqlite3_str_reset);
DEFINE_STUB(sqlite3_str_value);
DEFINE_STUB(sqlite3_str_vappendf);
DEFINE_STUB(sqlite3_strglob);
DEFINE_STUB(sqlite3_stricmp);
DEFINE_STUB(sqlite3_strlike);
DEFINE_STUB(sqlite3_strnicmp);
DEFINE_STUB(sqlite3_system_errno);
DEFINE_STUB(sqlite3_table_column_metadata);
DEFINE_STUB(sqlite3_test_control);
DEFINE_STUB(sqlite3_thread_cleanup);
DEFINE_STUB(sqlite3_threadsafe);
DEFINE_STUB(sqlite3_total_changes);
DEFINE_STUB(sqlite3_total_changes64);
DEFINE_STUB(sqlite3_trace);
DEFINE_STUB(sqlite3_trace_v2);
DEFINE_STUB(sqlite3_transfer_bindings);
DEFINE_STUB(sqlite3_txn_state);
DEFINE_STUB(sqlite3_unlock_notify);
DEFINE_STUB(sqlite3_update_hook);
DEFINE_STUB(sqlite3_uri_boolean);
DEFINE_STUB(sqlite3_uri_int64);
DEFINE_STUB(sqlite3_uri_key);
DEFINE_STUB(sqlite3_uri_parameter);
DEFINE_STUB(sqlite3_user_data);
DEFINE_STUB(sqlite3_value_blob);
DEFINE_STUB(sqlite3_value_bytes);
DEFINE_STUB(sqlite3_value_bytes16);
DEFINE_STUB(sqlite3_value_double);
DEFINE_STUB(sqlite3_value_dup);
DEFINE_STUB(sqlite3_value_free);
DEFINE_STUB(sqlite3_value_frombind);
DEFINE_STUB(sqlite3_value_int);
DEFINE_STUB(sqlite3_value_int64);
DEFINE_STUB(sqlite3_value_nochange);
DEFINE_STUB(sqlite3_value_numeric_type);
DEFINE_STUB(sqlite3_value_pointer);
DEFINE_STUB(sqlite3_value_subtype);
DEFINE_STUB(sqlite3_value_text);
DEFINE_STUB(sqlite3_value_text16);
DEFINE_STUB(sqlite3_value_text16be);
DEFINE_STUB(sqlite3_value_text16le);
DEFINE_STUB(sqlite3_value_type);
DEFINE_STUB(sqlite3_vfs_find);
DEFINE_STUB(sqlite3_vfs_register);
DEFINE_STUB(sqlite3_vfs_unregister);
DEFINE_STUB(sqlite3_vmprintf);
DEFINE_STUB(sqlite3_vsnprintf);
DEFINE_STUB(sqlite3_vtab_collation);
DEFINE_STUB(sqlite3_vtab_config);
DEFINE_STUB(sqlite3_vtab_distinct);
DEFINE_STUB(sqlite3_vtab_in);
DEFINE_STUB(sqlite3_vtab_in_first);
DEFINE_STUB(sqlite3_vtab_in_next);
DEFINE_STUB(sqlite3_vtab_nochange);
DEFINE_STUB(sqlite3_vtab_on_conflict);
DEFINE_STUB(sqlite3_vtab_rhs_value);
DEFINE_STUB(sqlite3_wal_autocheckpoint);
DEFINE_STUB(sqlite3_wal_checkpoint);
DEFINE_STUB(sqlite3_wal_checkpoint_v2);
DEFINE_STUB(sqlite3_wal_hook);
DEFINE_STUB(sqlite3_win32_set_directory);
DEFINE_STUB(sqlite3_win32_set_directory16);
DEFINE_STUB(sqlite3_win32_set_directory8);
