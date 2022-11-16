#include <stdio.h>
#include <stdlib.h>

#define SQLITE_OK	0
#define SQLITE_ERROR	1

typedef struct sqlite3 {
} sqlite3;

static struct sqlite3 *sqlite3_new(void)
{
	return malloc(sizeof(struct sqlite3));
}

#define TRACE() printf("TRACE %s\n", __func__)

#define STUB(func_name)					\
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
	printf("STUB %s\n", __func__);
	return SQLITE_OK;
}

int sqlite3_shutdown(void)
{
	printf("STUB %s\n", __func__);
	return SQLITE_OK;
}

int sqlite3_os_init(void)
{
	printf("STUB %s\n", __func__);
	return SQLITE_OK;
}

int sqlite3_os_end(void)
{
	printf("STUB %s\n", __func__);
	return SQLITE_OK;
}

/*
 * Error codes and messages.
 */

int sqlite3_errcode(sqlite3 *db)
{
	printf("STUB %s\n", __func__);
	return SQLITE_OK;
}

int sqlite3_extended_errcode(sqlite3 *db)
{
	printf("STUB %s\n", __func__);
	return SQLITE_OK;
}

const char *sqlite3_errmsg(sqlite3*)
{
	printf("STUB %s\n", __func__);
	return "unknown error";
}

const void *sqlite3_errmsg16(sqlite3*)
{
	printf("STUB %s\n", __func__);
	return NULL;
}

const char *sqlite3_errstr(int)
{
	printf("STUB %s\n", __func__);
	return NULL;
}

int sqlite3_error_offset(sqlite3 *db)
{
	printf("STUB %s\n", __func__);
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
 * Stubs.
 */

STUB(sqlite3_aggregate_context);
STUB(sqlite3_aggregate_count);
STUB(sqlite3_auto_extension);
STUB(sqlite3_autovacuum_pages);
STUB(sqlite3_backup_finish);
STUB(sqlite3_backup_init);
STUB(sqlite3_backup_pagecount);
STUB(sqlite3_backup_remaining);
STUB(sqlite3_backup_step);
STUB(sqlite3_bind_blob);
STUB(sqlite3_bind_blob64);
STUB(sqlite3_bind_double);
STUB(sqlite3_bind_int);
STUB(sqlite3_bind_int64);
STUB(sqlite3_bind_null);
STUB(sqlite3_bind_parameter_count);
STUB(sqlite3_bind_parameter_index);
STUB(sqlite3_bind_parameter_name);
STUB(sqlite3_bind_pointer);
STUB(sqlite3_bind_text);
STUB(sqlite3_bind_text16);
STUB(sqlite3_bind_text64);
STUB(sqlite3_bind_value);
STUB(sqlite3_bind_zeroblob);
STUB(sqlite3_bind_zeroblob64);
STUB(sqlite3_blob_bytes);
STUB(sqlite3_blob_close);
STUB(sqlite3_blob_open);
STUB(sqlite3_blob_read);
STUB(sqlite3_blob_reopen);
STUB(sqlite3_blob_write);
STUB(sqlite3_busy_handler);
STUB(sqlite3_busy_timeout);
STUB(sqlite3_cancel_auto_extension);
STUB(sqlite3_changes);
STUB(sqlite3_changes64);
STUB(sqlite3_clear_bindings);
STUB(sqlite3_close);
STUB(sqlite3_close_v2);
STUB(sqlite3_collation_needed);
STUB(sqlite3_collation_needed16);
STUB(sqlite3_column_blob);
STUB(sqlite3_column_bytes);
STUB(sqlite3_column_bytes16);
STUB(sqlite3_column_count);
STUB(sqlite3_column_database_name);
STUB(sqlite3_column_database_name16);
STUB(sqlite3_column_decltype);
STUB(sqlite3_column_decltype16);
STUB(sqlite3_column_double);
STUB(sqlite3_column_int);
STUB(sqlite3_column_int64);
STUB(sqlite3_column_name);
STUB(sqlite3_column_name16);
STUB(sqlite3_column_origin_name);
STUB(sqlite3_column_origin_name16);
STUB(sqlite3_column_table_name);
STUB(sqlite3_column_table_name16);
STUB(sqlite3_column_text);
STUB(sqlite3_column_text16);
STUB(sqlite3_column_type);
STUB(sqlite3_column_value);
STUB(sqlite3_commit_hook);
STUB(sqlite3_compileoption_get);
STUB(sqlite3_compileoption_used);
STUB(sqlite3_complete);
STUB(sqlite3_complete16);
STUB(sqlite3_config);
STUB(sqlite3_context_db_handle);
STUB(sqlite3_create_collation);
STUB(sqlite3_create_collation16);
STUB(sqlite3_create_collation_v2);
STUB(sqlite3_create_filename);
STUB(sqlite3_create_function);
STUB(sqlite3_create_function16);
STUB(sqlite3_create_function_v2);
STUB(sqlite3_create_module);
STUB(sqlite3_create_module_v2);
STUB(sqlite3_create_window_function);
STUB(sqlite3_data_count);
STUB(sqlite3_database_file_object);
STUB(sqlite3_db_cacheflush);
STUB(sqlite3_db_config);
STUB(sqlite3_db_filename);
STUB(sqlite3_db_handle);
STUB(sqlite3_db_mutex);
STUB(sqlite3_db_name);
STUB(sqlite3_db_readonly);
STUB(sqlite3_db_release_memory);
STUB(sqlite3_db_status);
STUB(sqlite3_declare_vtab);
STUB(sqlite3_deserialize);
STUB(sqlite3_drop_modules);
STUB(sqlite3_enable_load_extension);
STUB(sqlite3_enable_shared_cache);
STUB(sqlite3_exec);
STUB(sqlite3_expanded_sql);
STUB(sqlite3_expired);
STUB(sqlite3_extended_result_codes);
STUB(sqlite3_file_control);
STUB(sqlite3_filename_database);
STUB(sqlite3_filename_journal);
STUB(sqlite3_filename_wal);
STUB(sqlite3_finalize);
STUB(sqlite3_free);
STUB(sqlite3_free_filename);
STUB(sqlite3_free_table);
STUB(sqlite3_get_autocommit);
STUB(sqlite3_get_auxdata);
STUB(sqlite3_get_table);
STUB(sqlite3_global_recover);
STUB(sqlite3_hard_heap_limit64);
STUB(sqlite3_interrupt);
STUB(sqlite3_keyword_check);
STUB(sqlite3_keyword_count);
STUB(sqlite3_keyword_name);
STUB(sqlite3_last_insert_rowid);
STUB(sqlite3_limit);
STUB(sqlite3_load_extension);
STUB(sqlite3_log);
STUB(sqlite3_malloc);
STUB(sqlite3_malloc64);
STUB(sqlite3_memory_alarm);
STUB(sqlite3_memory_highwater);
STUB(sqlite3_memory_used);
STUB(sqlite3_mprintf);
STUB(sqlite3_msize);
STUB(sqlite3_mutex_alloc);
STUB(sqlite3_mutex_enter);
STUB(sqlite3_mutex_free);
STUB(sqlite3_mutex_held);
STUB(sqlite3_mutex_leave);
STUB(sqlite3_mutex_notheld);
STUB(sqlite3_mutex_try);
STUB(sqlite3_next_stmt);
STUB(sqlite3_normalized_sql);
STUB(sqlite3_overload_function);
STUB(sqlite3_prepare);
STUB(sqlite3_prepare16);
STUB(sqlite3_prepare16_v2);
STUB(sqlite3_prepare16_v3);
STUB(sqlite3_prepare_v2);
STUB(sqlite3_prepare_v3);
STUB(sqlite3_preupdate_blobwrite);
STUB(sqlite3_preupdate_count);
STUB(sqlite3_preupdate_depth);
STUB(sqlite3_preupdate_hook);
STUB(sqlite3_preupdate_new);
STUB(sqlite3_preupdate_old);
STUB(sqlite3_profile);
STUB(sqlite3_progress_handler);
STUB(sqlite3_randomness);
STUB(sqlite3_realloc);
STUB(sqlite3_realloc64);
STUB(sqlite3_release_memory);
STUB(sqlite3_reset);
STUB(sqlite3_reset_auto_extension);
STUB(sqlite3_result_blob);
STUB(sqlite3_result_blob64);
STUB(sqlite3_result_double);
STUB(sqlite3_result_error);
STUB(sqlite3_result_error16);
STUB(sqlite3_result_error_code);
STUB(sqlite3_result_error_nomem);
STUB(sqlite3_result_error_toobig);
STUB(sqlite3_result_int);
STUB(sqlite3_result_int64);
STUB(sqlite3_result_null);
STUB(sqlite3_result_pointer);
STUB(sqlite3_result_subtype);
STUB(sqlite3_result_text);
STUB(sqlite3_result_text16);
STUB(sqlite3_result_text16be);
STUB(sqlite3_result_text16le);
STUB(sqlite3_result_text64);
STUB(sqlite3_result_value);
STUB(sqlite3_result_zeroblob);
STUB(sqlite3_result_zeroblob64);
STUB(sqlite3_rollback_hook);
STUB(sqlite3_serialize);
STUB(sqlite3_set_authorizer);
STUB(sqlite3_set_auxdata);
STUB(sqlite3_set_last_insert_rowid);
STUB(sqlite3_sleep);
STUB(sqlite3_snapshot_cmp);
STUB(sqlite3_snapshot_free);
STUB(sqlite3_snapshot_get);
STUB(sqlite3_snapshot_open);
STUB(sqlite3_snapshot_recover);
STUB(sqlite3_snprintf);
STUB(sqlite3_soft_heap_limit);
STUB(sqlite3_soft_heap_limit64);
STUB(sqlite3_sourceid);
STUB(sqlite3_sql);
STUB(sqlite3_status);
STUB(sqlite3_status64);
STUB(sqlite3_step);
STUB(sqlite3_stmt_busy);
STUB(sqlite3_stmt_isexplain);
STUB(sqlite3_stmt_readonly);
STUB(sqlite3_stmt_scanstatus);
STUB(sqlite3_stmt_scanstatus_reset);
STUB(sqlite3_stmt_status);
STUB(sqlite3_str_append);
STUB(sqlite3_str_appendall);
STUB(sqlite3_str_appendchar);
STUB(sqlite3_str_appendf);
STUB(sqlite3_str_errcode);
STUB(sqlite3_str_finish);
STUB(sqlite3_str_length);
STUB(sqlite3_str_new);
STUB(sqlite3_str_reset);
STUB(sqlite3_str_value);
STUB(sqlite3_str_vappendf);
STUB(sqlite3_strglob);
STUB(sqlite3_stricmp);
STUB(sqlite3_strlike);
STUB(sqlite3_strnicmp);
STUB(sqlite3_system_errno);
STUB(sqlite3_table_column_metadata);
STUB(sqlite3_test_control);
STUB(sqlite3_thread_cleanup);
STUB(sqlite3_threadsafe);
STUB(sqlite3_total_changes);
STUB(sqlite3_total_changes64);
STUB(sqlite3_trace);
STUB(sqlite3_trace_v2);
STUB(sqlite3_transfer_bindings);
STUB(sqlite3_txn_state);
STUB(sqlite3_unlock_notify);
STUB(sqlite3_update_hook);
STUB(sqlite3_uri_boolean);
STUB(sqlite3_uri_int64);
STUB(sqlite3_uri_key);
STUB(sqlite3_uri_parameter);
STUB(sqlite3_user_data);
STUB(sqlite3_value_blob);
STUB(sqlite3_value_bytes);
STUB(sqlite3_value_bytes16);
STUB(sqlite3_value_double);
STUB(sqlite3_value_dup);
STUB(sqlite3_value_free);
STUB(sqlite3_value_frombind);
STUB(sqlite3_value_int);
STUB(sqlite3_value_int64);
STUB(sqlite3_value_nochange);
STUB(sqlite3_value_numeric_type);
STUB(sqlite3_value_pointer);
STUB(sqlite3_value_subtype);
STUB(sqlite3_value_text);
STUB(sqlite3_value_text16);
STUB(sqlite3_value_text16be);
STUB(sqlite3_value_text16le);
STUB(sqlite3_value_type);
STUB(sqlite3_vfs_find);
STUB(sqlite3_vfs_register);
STUB(sqlite3_vfs_unregister);
STUB(sqlite3_vmprintf);
STUB(sqlite3_vsnprintf);
STUB(sqlite3_vtab_collation);
STUB(sqlite3_vtab_config);
STUB(sqlite3_vtab_distinct);
STUB(sqlite3_vtab_in);
STUB(sqlite3_vtab_in_first);
STUB(sqlite3_vtab_in_next);
STUB(sqlite3_vtab_nochange);
STUB(sqlite3_vtab_on_conflict);
STUB(sqlite3_vtab_rhs_value);
STUB(sqlite3_wal_autocheckpoint);
STUB(sqlite3_wal_checkpoint);
STUB(sqlite3_wal_checkpoint_v2);
STUB(sqlite3_wal_hook);
STUB(sqlite3_win32_set_directory);
STUB(sqlite3_win32_set_directory16);
STUB(sqlite3_win32_set_directory8);
