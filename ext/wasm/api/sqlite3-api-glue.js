/*
  2022-07-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file glues together disparate pieces of JS which are loaded in
  previous steps of the sqlite3-api.js bootstrapping process:
  sqlite3-api-prologue.js, whwasmutil.js, and jaccwabyt.js. It
  initializes the main API pieces so that the downstream components
  (e.g. sqlite3-api-oo1.js) have all that they need.
*/
self.sqlite3ApiBootstrap.initializers.push(function(sqlite3){
  'use strict';
  const toss = (...args)=>{throw new Error(args.join(' '))};
  const toss3 = sqlite3.SQLite3Error.toss;
  const capi = sqlite3.capi, wasm = sqlite3.wasm, util = sqlite3.util;
  self.WhWasmUtilInstaller(wasm);
  delete self.WhWasmUtilInstaller;

  if(0){
    /**
       Please keep this block around as a maintenance reminder
       that we cannot rely on this type of check.

       This block fails on Safari, per a report at
       https://sqlite.org/forum/forumpost/e5b20e1feb.

       It turns out that what Safari serves from the indirect function
       table (e.g. wasm.functionEntry(X)) is anonymous functions which
       wrap the WASM functions, rather than returning the WASM
       functions themselves. That means comparison of such functions
       is useless for determining whether or not we have a specific
       function from wasm.exports. i.e. if function X is indirection
       function table entry N then wasm.exports.X is not equal to
       wasm.functionEntry(N) in Safari, despite being so in the other
       browsers.
    */
    /**
       Find a mapping for SQLITE_WASM_DEALLOC, which the API
       guarantees is a WASM pointer to the same underlying function as
       wasm.dealloc() (noting that wasm.dealloc() is permitted to be a
       JS wrapper around the WASM function). There is unfortunately no
       O(1) algorithm for finding this pointer: we have to walk the
       WASM indirect function table to find it. However, experience
       indicates that that particular function is always very close to
       the front of the table (it's been entry #3 in all relevant
       tests).
    */
    const dealloc = wasm.exports[sqlite3.config.deallocExportName];
    const nFunc = wasm.functionTable().length;
    let i;
    for(i = 0; i < nFunc; ++i){
      const e = wasm.functionEntry(i);
      if(dealloc === e){
        capi.SQLITE_WASM_DEALLOC = i;
        break;
      }
    }
    if(dealloc !== wasm.functionEntry(capi.SQLITE_WASM_DEALLOC)){
      toss("Internal error: cannot find function pointer for SQLITE_WASM_DEALLOC.");
    }
  }

  /**
     Signatures for the WASM-exported C-side functions. Each entry
     is an array with 2+ elements:

     [ "c-side name",
       "result type" (wasm.xWrap() syntax),
       [arg types in xWrap() syntax]
       // ^^^ this needn't strictly be an array: it can be subsequent
       // elements instead: [x,y,z] is equivalent to x,y,z
     ]

     Note that support for the API-specific data types in the
     result/argument type strings gets plugged in at a later phase in
     the API initialization process.
  */
  wasm.bindingSignatures = [
    // Please keep these sorted by function name!
    ["sqlite3_aggregate_context","void*", "sqlite3_context*", "int"],
    /* sqlite3_auto_extension() has a hand-written binding. */
    /* sqlite3_bind_blob() and sqlite3_bind_text() have hand-written
       bindings to permit more flexible inputs. */
    ["sqlite3_bind_double","int", "sqlite3_stmt*", "int", "f64"],
    ["sqlite3_bind_int","int", "sqlite3_stmt*", "int", "int"],
    ["sqlite3_bind_null",undefined, "sqlite3_stmt*", "int"],
    ["sqlite3_bind_parameter_count", "int", "sqlite3_stmt*"],
    ["sqlite3_bind_parameter_index","int", "sqlite3_stmt*", "string"],
    ["sqlite3_bind_pointer", "int",
     "sqlite3_stmt*", "int", "*", "string:static", "*"],
    ["sqlite3_busy_handler","int", [
      "sqlite3*",
      new wasm.xWrap.FuncPtrAdapter({
        signature: 'i(pi)',
        contextKey: (argv,argIndex)=>argv[0/* sqlite3* */]
      }),
      "*"
    ]],
    ["sqlite3_busy_timeout","int", "sqlite3*", "int"],
    /* sqlite3_cancel_auto_extension() has a hand-written binding. */
    /* sqlite3_close_v2() is implemented by hand to perform some
       extra work. */
    ["sqlite3_changes", "int", "sqlite3*"],
    ["sqlite3_clear_bindings","int", "sqlite3_stmt*"],
    ["sqlite3_collation_needed", "int", "sqlite3*", "*", "*"/*=>v(ppis)*/],
    ["sqlite3_column_blob","*", "sqlite3_stmt*", "int"],
    ["sqlite3_column_bytes","int", "sqlite3_stmt*", "int"],
    ["sqlite3_column_count", "int", "sqlite3_stmt*"],
    ["sqlite3_column_double","f64", "sqlite3_stmt*", "int"],
    ["sqlite3_column_int","int", "sqlite3_stmt*", "int"],
    ["sqlite3_column_name","string", "sqlite3_stmt*", "int"],
    ["sqlite3_column_text","string", "sqlite3_stmt*", "int"],
    ["sqlite3_column_type","int", "sqlite3_stmt*", "int"],
    ["sqlite3_column_value","sqlite3_value*", "sqlite3_stmt*", "int"],
    ["sqlite3_commit_hook", "void*", [
      "sqlite3*",
      new wasm.xWrap.FuncPtrAdapter({
        name: 'sqlite3_commit_hook',
        signature: 'i(p)',
        contextKey: (argv)=>argv[0/* sqlite3* */]
      }),
      '*'
    ]],
    ["sqlite3_compileoption_get", "string", "int"],
    ["sqlite3_compileoption_used", "int", "string"],
    ["sqlite3_complete", "int", "string:flexible"],
    ["sqlite3_context_db_handle", "sqlite3*", "sqlite3_context*"],

    /* sqlite3_create_function(), sqlite3_create_function_v2(), and
       sqlite3_create_window_function() use hand-written bindings to
       simplify handling of their function-type arguments. */
    /* sqlite3_create_collation() and sqlite3_create_collation_v2()
       use hand-written bindings to simplify passing of the callback
       function.
      ["sqlite3_create_collation", "int",
     "sqlite3*", "string", "int",//SQLITE_UTF8 is the only legal value
     "*", "*"],
    ["sqlite3_create_collation_v2", "int",
     "sqlite3*", "string", "int",//SQLITE_UTF8 is the only legal value
     "*", "*", "*"],
    */
    ["sqlite3_data_count", "int", "sqlite3_stmt*"],
    ["sqlite3_db_filename", "string", "sqlite3*", "string"],
    ["sqlite3_db_handle", "sqlite3*", "sqlite3_stmt*"],
    ["sqlite3_db_name", "string", "sqlite3*", "int"],
    ["sqlite3_db_status", "int", "sqlite3*", "int", "*", "*", "int"],
    ["sqlite3_errcode", "int", "sqlite3*"],
    ["sqlite3_errmsg", "string", "sqlite3*"],
    ["sqlite3_error_offset", "int", "sqlite3*"],
    ["sqlite3_errstr", "string", "int"],
    ["sqlite3_exec", "int", [
      "sqlite3*", "string:flexible",
      new wasm.xWrap.FuncPtrAdapter({
        signature: 'i(pipp)',
        bindScope: 'transient',
        callProxy: (callback)=>{
          let aNames;
          return (pVoid, nCols, pColVals, pColNames)=>{
            try {
              const aVals = wasm.cArgvToJs(nCols, pColVals);
              if(!aNames) aNames = wasm.cArgvToJs(nCols, pColNames);
              return callback(aVals, aNames) | 0;
            }catch(e){
              /* If we set the db error state here, the higher-level
                 exec() call replaces it with its own, so we have no way
                 of reporting the exception message except the console. We
                 must not propagate exceptions through the C API. Though
                 we make an effort to report OOM here, sqlite3_exec()
                 translates that into SQLITE_ABORT as well. */
              return e.resultCode || capi.SQLITE_ERROR;
            }
          }
        }
      }),
      "*", "**"
    ]],
    ["sqlite3_expanded_sql", "string", "sqlite3_stmt*"],
    ["sqlite3_extended_errcode", "int", "sqlite3*"],
    ["sqlite3_extended_result_codes", "int", "sqlite3*", "int"],
    ["sqlite3_file_control", "int", "sqlite3*", "string", "int", "*"],
    ["sqlite3_finalize", "int", "sqlite3_stmt*"],
    ["sqlite3_free", undefined,"*"],
    ["sqlite3_get_auxdata", "*", "sqlite3_context*", "int"],
    ["sqlite3_initialize", undefined],
    /*["sqlite3_interrupt", undefined, "sqlite3*"
       ^^^ we cannot actually currently support this because JS is
        single-threaded and we don't have a portable way to access a DB
        from 2 SharedWorkers concurrently. ],*/
    ["sqlite3_keyword_count", "int"],
    ["sqlite3_keyword_name", "int", ["int", "**", "*"]],
    ["sqlite3_keyword_check", "int", ["string", "int"]],
    ["sqlite3_libversion", "string"],
    ["sqlite3_libversion_number", "int"],
    ["sqlite3_limit", "int", ["sqlite3*", "int", "int"]],
    ["sqlite3_malloc", "*","int"],
    ["sqlite3_open", "int", "string", "*"],
    ["sqlite3_open_v2", "int", "string", "*", "int", "string"],
    /* sqlite3_prepare_v2() and sqlite3_prepare_v3() are handled
       separately due to us requiring two different sets of semantics
       for those, depending on how their SQL argument is provided. */
    /* sqlite3_randomness() uses a hand-written wrapper to extend
       the range of supported argument types. */
    ["sqlite3_progress_handler", undefined, [
      "sqlite3*", "int", new wasm.xWrap.FuncPtrAdapter({
        name: 'xProgressHandler',
        signature: 'i(p)',
        bindScope: 'context',
        contextKey: (argv,argIndex)=>argv[0/* sqlite3* */]
      }), "*"
    ]],
    ["sqlite3_realloc", "*","*","int"],
    ["sqlite3_reset", "int", "sqlite3_stmt*"],
    /* sqlite3_reset_auto_extension() has a hand-written binding. */
    ["sqlite3_result_blob", undefined, "sqlite3_context*", "*", "int", "*"],
    ["sqlite3_result_double", undefined, "sqlite3_context*", "f64"],
    ["sqlite3_result_error", undefined, "sqlite3_context*", "string", "int"],
    ["sqlite3_result_error_code", undefined, "sqlite3_context*", "int"],
    ["sqlite3_result_error_nomem", undefined, "sqlite3_context*"],
    ["sqlite3_result_error_toobig", undefined, "sqlite3_context*"],
    ["sqlite3_result_int", undefined, "sqlite3_context*", "int"],
    ["sqlite3_result_null", undefined, "sqlite3_context*"],
    ["sqlite3_result_pointer", undefined,
     "sqlite3_context*", "*", "string:static", "*"],
    ["sqlite3_result_subtype", undefined, "sqlite3_value*", "int"],
    ["sqlite3_result_text", undefined, "sqlite3_context*", "string", "int", "*"],
    ["sqlite3_result_zeroblob", undefined, "sqlite3_context*", "int"],
    ["sqlite3_rollback_hook", "void*", [
      "sqlite3*",
      new wasm.xWrap.FuncPtrAdapter({
        name: 'sqlite3_rollback_hook',
        signature: 'v(p)',
        contextKey: (argv)=>argv[0/* sqlite3* */]
      }),
      '*'
    ]],
    ["sqlite3_set_authorizer", "int", [
      "sqlite3*",
      new wasm.xWrap.FuncPtrAdapter({
        name: "sqlite3_set_authorizer::xAuth",
        signature: "i(pi"+"ssss)",
        contextKey: (argv, argIndex)=>argv[0/*(sqlite3*)*/],
        callProxy: (callback)=>{
          return (pV, iCode, s0, s1, s2, s3)=>{
            try{
              s0 = s0 && wasm.cstrToJs(s0); s1 = s1 && wasm.cstrToJs(s1);
              s2 = s2 && wasm.cstrToJs(s2); s3 = s3 && wasm.cstrToJs(s3);
              return callback(pV, iCode, s0, s1, s2, s3) || 0;
            }catch(e){
              return e.resultCode || capi.SQLITE_ERROR;
            }
          }
        }
      }),
      "*"/*pUserData*/
    ]],
    ["sqlite3_set_auxdata", undefined, [
      "sqlite3_context*", "int", "*",
      new wasm.xWrap.FuncPtrAdapter({
        name: 'xDestroyAuxData',
        signature: 'v(*)',
        contextKey: (argv, argIndex)=>argv[0/* sqlite3_context* */]
      })
    ]],
    ["sqlite3_shutdown", undefined],
    ["sqlite3_sourceid", "string"],
    ["sqlite3_sql", "string", "sqlite3_stmt*"],
    ["sqlite3_status", "int", "int", "*", "*", "int"],
    ["sqlite3_step", "int", "sqlite3_stmt*"],
    ["sqlite3_stmt_isexplain", "int", ["sqlite3_stmt*"]],
    ["sqlite3_stmt_readonly", "int", ["sqlite3_stmt*"]],
    ["sqlite3_stmt_status", "int", "sqlite3_stmt*", "int", "int"],
    ["sqlite3_strglob", "int", "string","string"],
    ["sqlite3_stricmp", "int", "string", "string"],
    ["sqlite3_strlike", "int", "string", "string","int"],
    ["sqlite3_strnicmp", "int", "string", "string", "int"],
    ["sqlite3_table_column_metadata", "int",
     "sqlite3*", "string", "string", "string",
     "**", "**", "*", "*", "*"],
    ["sqlite3_total_changes", "int", "sqlite3*"],
    ["sqlite3_trace_v2", "int", [
      "sqlite3*", "int",
      new wasm.xWrap.FuncPtrAdapter({
        name: 'sqlite3_trace_v2::callback',
        signature: 'i(ippp)',
        contextKey: (argv,argIndex)=>argv[0/* sqlite3* */]
      }),
      "*"
    ]],
    ["sqlite3_txn_state", "int", ["sqlite3*","string"]],
    /* Note that sqlite3_uri_...() have very specific requirements for
       their first C-string arguments, so we cannot perform any value
       conversion on those. */
    ["sqlite3_uri_boolean", "int", "sqlite3_filename", "string", "int"],
    ["sqlite3_uri_key", "string", "sqlite3_filename", "int"],
    ["sqlite3_uri_parameter", "string", "sqlite3_filename", "string"],
    ["sqlite3_user_data","void*", "sqlite3_context*"],
    ["sqlite3_value_blob", "*", "sqlite3_value*"],
    ["sqlite3_value_bytes","int", "sqlite3_value*"],
    ["sqlite3_value_double","f64", "sqlite3_value*"],
    ["sqlite3_value_dup", "sqlite3_value*", "sqlite3_value*"],
    ["sqlite3_value_free", undefined, "sqlite3_value*"],
    ["sqlite3_value_frombind", "int", "sqlite3_value*"],
    ["sqlite3_value_int","int", "sqlite3_value*"],
    ["sqlite3_value_nochange", "int", "sqlite3_value*"],
    ["sqlite3_value_numeric_type", "int", "sqlite3_value*"],
    ["sqlite3_value_pointer", "*", "sqlite3_value*", "string:static"],
    ["sqlite3_value_subtype", "int", "sqlite3_value*"],
    ["sqlite3_value_text", "string", "sqlite3_value*"],
    ["sqlite3_value_type", "int", "sqlite3_value*"],
    ["sqlite3_vfs_find", "*", "string"],
    ["sqlite3_vfs_register", "int", "sqlite3_vfs*", "int"],
    ["sqlite3_vfs_unregister", "int", "sqlite3_vfs*"]
  ]/*wasm.bindingSignatures*/;

  if(false && wasm.compileOptionUsed('SQLITE_ENABLE_NORMALIZE')){
    /* ^^^ "the problem" is that this is an option feature and the
       build-time function-export list does not currently take
       optional features into account. */
    wasm.bindingSignatures.push(["sqlite3_normalized_sql", "string", "sqlite3_stmt*"]);
  }

  /**
     Functions which require BigInt (int64) support are separated from
     the others because we need to conditionally bind them or apply
     dummy impls, depending on the capabilities of the environment.

     Note that not all of these functions directly require int64
     but are only for use with APIs which require int64. For example,
     the vtab-related functions.
  */
  wasm.bindingSignatures.int64 = [
    ["sqlite3_bind_int64","int", ["sqlite3_stmt*", "int", "i64"]],
    ["sqlite3_changes64","i64", ["sqlite3*"]],
    ["sqlite3_column_int64","i64", ["sqlite3_stmt*", "int"]],
    ["sqlite3_create_module", "int",
     ["sqlite3*","string","sqlite3_module*","*"]],
    ["sqlite3_create_module_v2", "int",
     ["sqlite3*","string","sqlite3_module*","*","*"]],
    ["sqlite3_declare_vtab", "int", ["sqlite3*", "string:flexible"]],
    ["sqlite3_deserialize", "int", "sqlite3*", "string", "*", "i64", "i64", "int"]
    /* Careful! Short version: de/serialize() are problematic because they
       might use a different allocator than the user for managing the
       deserialized block. de/serialize() are ONLY safe to use with
       sqlite3_malloc(), sqlite3_free(), and its 64-bit variants. */,
    ["sqlite3_drop_modules", "int", ["sqlite3*", "**"]],
    ["sqlite3_last_insert_rowid", "i64", ["sqlite3*"]],
    ["sqlite3_malloc64", "*","i64"],
    ["sqlite3_msize", "i64", "*"],
    ["sqlite3_overload_function", "int", ["sqlite3*","string","int"]],
    ["sqlite3_preupdate_blobwrite", "int", "sqlite3*"],
    ["sqlite3_preupdate_count", "int", "sqlite3*"],
    ["sqlite3_preupdate_depth", "int", "sqlite3*"],
    ["sqlite3_preupdate_hook", "*", [
      "sqlite3*",
      new wasm.xWrap.FuncPtrAdapter({
        name: 'sqlite3_preupdate_hook',
        signature: "v(ppippjj)",
        contextKey: (argv)=>argv[0/* sqlite3* */],
        callProxy: (callback)=>{
          return (p,db,op,zDb,zTbl,iKey1,iKey2)=>{
            callback(p, db, op, wasm.cstrToJs(zDb), wasm.cstrToJs(zTbl),
                     iKey1, iKey2);
          };
        }
      }),
      "*"
    ]],
    ["sqlite3_preupdate_new", "int", ["sqlite3*", "int", "**"]],
    ["sqlite3_preupdate_old", "int", ["sqlite3*", "int", "**"]],
    ["sqlite3_realloc64", "*","*", "i64"],
    ["sqlite3_result_int64", undefined, "*", "i64"],
    ["sqlite3_result_zeroblob64", "int", "*", "i64"],
    ["sqlite3_serialize","*", "sqlite3*", "string", "*", "int"],
    ["sqlite3_set_last_insert_rowid", undefined, ["sqlite3*", "i64"]],
    ["sqlite3_status64", "int", "int", "*", "*", "int"],
    ["sqlite3_total_changes64", "i64", ["sqlite3*"]],
    ["sqlite3_update_hook", "*", [
      "sqlite3*",
      new wasm.xWrap.FuncPtrAdapter({
        name: 'sqlite3_update_hook',
        signature: "v(iippj)",
        contextKey: (argv)=>argv[0/* sqlite3* */],
        callProxy: (callback)=>{
          return (p,op,z0,z1,rowid)=>{
            callback(p, op, wasm.cstrToJs(z0), wasm.cstrToJs(z1), rowid);
          };
        }
      }),
      "*"
    ]],
    ["sqlite3_uri_int64", "i64", ["sqlite3_filename", "string", "i64"]],
    ["sqlite3_value_int64","i64", "sqlite3_value*"],
    ["sqlite3_vtab_collation","string","sqlite3_index_info*","int"],
    ["sqlite3_vtab_distinct","int", "sqlite3_index_info*"],
    ["sqlite3_vtab_in","int", "sqlite3_index_info*", "int", "int"],
    ["sqlite3_vtab_in_first", "int", "sqlite3_value*", "**"],
    ["sqlite3_vtab_in_next", "int", "sqlite3_value*", "**"],
    /*["sqlite3_vtab_config" is variadic and requires a hand-written
      proxy.] */
    ["sqlite3_vtab_nochange","int", "sqlite3_context*"],
    ["sqlite3_vtab_on_conflict","int", "sqlite3*"],
    ["sqlite3_vtab_rhs_value","int", "sqlite3_index_info*", "int", "**"]
  ];

  // Add session/changeset APIs...
  if(wasm.bigIntEnabled && !!wasm.exports.sqlite3changegroup_add){
    /* ACHTUNG: 2022-12-23: the session/changeset API bindings are
       COMPLETELY UNTESTED. */
    /**
       FuncPtrAdapter options for session-related callbacks with the
       native signature "i(ps)". This proxy converts the 2nd argument
       from a C string to a JS string before passing the arguments on
       to the client-provided JS callback.
    */
    const __ipsProxy = {
      signature: 'i(ps)',
      callProxy:(callback)=>{
        return (p,s)=>{
          try{return callback(p, wasm.cstrToJs(s)) | 0}
          catch(e){return e.resultCode || capi.SQLITE_ERROR}
        }
      }
    };

    wasm.bindingSignatures.int64.push(...[
      ['sqlite3changegroup_add', 'int', ['sqlite3_changegroup*', 'int', 'void*']],
      ['sqlite3changegroup_add_strm', 'int', [
        'sqlite3_changegroup*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xInput', signature: 'i(ppp)', bindScope: 'transient'
        }),
        'void*'
      ]],
      ['sqlite3changegroup_delete', undefined, ['sqlite3_changegroup*']],
      ['sqlite3changegroup_new', 'int', ['**']],
      ['sqlite3changegroup_output', 'int', ['sqlite3_changegroup*', 'int*', '**']],
      ['sqlite3changegroup_output_strm', 'int', [
        'sqlite3_changegroup*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xOutput', signature: 'i(ppi)', bindScope: 'transient'
        }),
        'void*'
      ]],
      ['sqlite3changeset_apply', 'int', [
        'sqlite3*', 'int', 'void*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xFilter', bindScope: 'transient', ...__ipsProxy
        }),
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xConflict', signature: 'i(pip)', bindScope: 'transient'
        }),
        'void*'
      ]],
      ['sqlite3changeset_apply_strm', 'int', [
        'sqlite3*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xInput', signature: 'i(ppp)', bindScope: 'transient'
        }),
        'void*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xFilter', bindScope: 'transient', ...__ipsProxy
        }),
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xConflict', signature: 'i(pip)', bindScope: 'transient'
        }),
        'void*'
      ]],
      ['sqlite3changeset_apply_v2', 'int', [
        'sqlite3*', 'int', 'void*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xFilter', bindScope: 'transient', ...__ipsProxy
        }),
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xConflict', signature: 'i(pip)', bindScope: 'transient'
        }),
        'void*', '**', 'int*', 'int'

      ]],
      ['sqlite3changeset_apply_v2_strm', 'int', [
        'sqlite3*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xInput', signature: 'i(ppp)', bindScope: 'transient'
        }),
        'void*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xFilter', bindScope: 'transient', ...__ipsProxy
        }),
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xConflict', signature: 'i(pip)', bindScope: 'transient'
        }),
        'void*', '**', 'int*', 'int'
      ]],
      ['sqlite3changeset_concat', 'int', ['int','void*', 'int', 'void*', 'int*', '**']],
      ['sqlite3changeset_concat_strm', 'int', [
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xInputA', signature: 'i(ppp)', bindScope: 'transient'
        }),
        'void*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xInputB', signature: 'i(ppp)', bindScope: 'transient'
        }),
        'void*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xOutput', signature: 'i(ppi)', bindScope: 'transient'
        }),
        'void*'
      ]],
      ['sqlite3changeset_conflict', 'int', ['sqlite3_changeset_iter*', 'int', '**']],
      ['sqlite3changeset_finalize', 'int', ['sqlite3_changeset_iter*']],
      ['sqlite3changeset_fk_conflicts', 'int', ['sqlite3_changeset_iter*', 'int*']],
      ['sqlite3changeset_invert', 'int', ['int', 'void*', 'int*', '**']],
      ['sqlite3changeset_invert_strm', 'int', [
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xInput', signature: 'i(ppp)', bindScope: 'transient'
        }),
        'void*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xOutput', signature: 'i(ppi)', bindScope: 'transient'
        }),
        'void*'
      ]],
      ['sqlite3changeset_new', 'int', ['sqlite3_changeset_iter*', 'int', '**']],
      ['sqlite3changeset_next', 'int', ['sqlite3_changeset_iter*']],
      ['sqlite3changeset_old', 'int', ['sqlite3_changeset_iter*', 'int', '**']],
      ['sqlite3changeset_op', 'int', [
        'sqlite3_changeset_iter*', '**', 'int*', 'int*','int*'
      ]],
      ['sqlite3changeset_pk', 'int', ['sqlite3_changeset_iter*', '**', 'int*']],
      ['sqlite3changeset_start', 'int', ['**', 'int', '*']],
      ['sqlite3changeset_start_strm', 'int', [
        '**',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xInput', signature: 'i(ppp)', bindScope: 'transient'
        }),
        'void*'
      ]],
      ['sqlite3changeset_start_v2', 'int', ['**', 'int', '*', 'int']],
      ['sqlite3changeset_start_v2_strm', 'int', [
        '**',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xInput', signature: 'i(ppp)', bindScope: 'transient'
        }),
        'void*', 'int'
      ]],
      ['sqlite3session_attach', 'int', ['sqlite3_session*', 'string']],
      ['sqlite3session_changeset', 'int', ['sqlite3_session*', 'int*', '**']],
      ['sqlite3session_changeset_size', 'i64', ['sqlite3_session*']],
      ['sqlite3session_changeset_strm', 'int', [
        'sqlite3_session*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xOutput', signature: 'i(ppp)', bindScope: 'transient'
        }),
        'void*'
      ]],
      ['sqlite3session_config', 'int', ['int', 'void*']],
      ['sqlite3session_create', 'int', ['sqlite3*', 'string', '**']],
      //sqlite3session_delete() is bound manually
      ['sqlite3session_diff', 'int', ['sqlite3_session*', 'string', 'string', '**']],
      ['sqlite3session_enable', 'int', ['sqlite3_session*', 'int']],
      ['sqlite3session_indirect', 'int', ['sqlite3_session*', 'int']],
      ['sqlite3session_isempty', 'int', ['sqlite3_session*']],
      ['sqlite3session_memory_used', 'i64', ['sqlite3_session*']],
      ['sqlite3session_object_config', 'int', ['sqlite3_session*', 'int', 'void*']],
      ['sqlite3session_patchset', 'int', ['sqlite3_session*', '*', '**']],
      ['sqlite3session_patchset_strm', 'int', [
        'sqlite3_session*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xOutput', signature: 'i(ppp)', bindScope: 'transient'
        }),
        'void*'
      ]],
      ['sqlite3session_table_filter', undefined, [
        'sqlite3_session*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xFilter', ...__ipsProxy,
          contextKey: (argv,argIndex)=>argv[0/* (sqlite3_session*) */]
        }),
        '*'
      ]]
    ]);
  }/*session/changeset APIs*/

  /**
     Functions which are intended solely for API-internal use by the
     WASM components, not client code. These get installed into
     sqlite3.wasm. Some of them get exposed to clients via variants
     named sqlite3_js_...().
  */
  wasm.bindingSignatures.wasm = [
    ["sqlite3_wasm_db_reset", "int", "sqlite3*"],
    ["sqlite3_wasm_db_vfs", "sqlite3_vfs*", "sqlite3*","string"],
    ["sqlite3_wasm_vfs_create_file", "int",
     "sqlite3_vfs*","string","*", "int"],
    ["sqlite3_wasm_vfs_unlink", "int", "sqlite3_vfs*","string"]
  ];

  /**
     Install JS<->C struct bindings for the non-opaque struct types we
     need... */
  sqlite3.StructBinder = self.Jaccwabyt({
    heap: 0 ? wasm.memory : wasm.heap8u,
    alloc: wasm.alloc,
    dealloc: wasm.dealloc,
    bigIntEnabled: wasm.bigIntEnabled,
    memberPrefix: /* Never change this: this prefix is baked into any
                     amount of code and client-facing docs. */ '$'
  });
  delete self.Jaccwabyt;

  {// wasm.xWrap() bindings...

    /* Convert Arrays and certain TypedArrays to strings for
       'string:flexible'-type arguments */
    const __xString = wasm.xWrap.argAdapter('string');
    wasm.xWrap.argAdapter(
      'string:flexible', (v)=>__xString(util.flexibleString(v))
    );

    /**
       The 'string:static' argument adapter treats its argument as
       either...

       - WASM pointer: assumed to be a long-lived C-string which gets
         returned as-is.

       - Anything else: gets coerced to a JS string for use as a map
         key. If a matching entry is found (as described next), it is
         returned, else wasm.allocCString() is used to create a a new
         string, map its pointer to (''+v) for the remainder of the
         application's life, and returns that pointer value for this
         call and all future calls which are passed a
         string-equivalent argument.

       Use case: sqlite3_bind_pointer() and sqlite3_result_pointer()
       call for "a static string and preferably a string
       literal". This converter is used to ensure that the string
       value seen by those functions is long-lived and behaves as they
       need it to.
    */
    wasm.xWrap.argAdapter(
      'string:static',
      function(v){
        if(wasm.isPtr(v)) return v;
        v = ''+v;
        let rc = this[v];
        return rc || (this[v] = wasm.allocCString(v));
      }.bind(Object.create(null))
    );

    /**
       Add some descriptive xWrap() aliases for '*' intended to (A)
       initially improve readability/correctness of
       wasm.bindingSignatures and (B) provide automatic conversion
       from higher-level representations, e.g. capi.sqlite3_vfs to
       `sqlite3_vfs*` via capi.sqlite3_vfs.pointer.
    */
    const __xArgPtr = wasm.xWrap.argAdapter('*');
    const nilType = function(){}/*a class no value can ever be an instance of*/;
    wasm.xWrap.argAdapter('sqlite3_filename', __xArgPtr)
    ('sqlite3_context*', __xArgPtr)
    ('sqlite3_value*', __xArgPtr)
    ('void*', __xArgPtr)
    ('sqlite3_changegroup*', __xArgPtr)
    ('sqlite3_changeset_iter*', __xArgPtr)
    //('sqlite3_rebaser*', __xArgPtr)
    ('sqlite3_session*', __xArgPtr)
    ('sqlite3_stmt*', (v)=>
      __xArgPtr((v instanceof (sqlite3?.oo1?.Stmt || nilType))
           ? v.pointer : v))
    ('sqlite3*', (v)=>
      __xArgPtr((v instanceof (sqlite3?.oo1?.DB || nilType))
           ? v.pointer : v))
    ('sqlite3_index_info*', (v)=>
      __xArgPtr((v instanceof (capi.sqlite3_index_info || nilType))
           ? v.pointer : v))
    ('sqlite3_module*', (v)=>
      __xArgPtr((v instanceof (capi.sqlite3_module || nilType))
           ? v.pointer : v))
    /**
       `sqlite3_vfs*`:

       - v is-a string: use the result of sqlite3_vfs_find(v) but
         throw if it returns 0.
       - v is-a capi.sqlite3_vfs: use v.pointer.
       - Else return the same as the `'*'` argument conversion.
    */
    ('sqlite3_vfs*', (v)=>{
      if('string'===typeof v){
        /* A NULL sqlite3_vfs pointer will be treated as the default
           VFS in many contexts. We specifically do not want that
           behavior here. */
        return capi.sqlite3_vfs_find(v)
          || sqlite3.SQLite3Error.toss(
            capi.SQLITE_NOTFOUND,
            "Unknown sqlite3_vfs name:", v
          );
      }
      return __xArgPtr((v instanceof (capi.sqlite3_vfs || nilType))
                       ? v.pointer : v);
    });

    const __xRcPtr = wasm.xWrap.resultAdapter('*');
    wasm.xWrap.resultAdapter('sqlite3*', __xRcPtr)
    ('sqlite3_context*', __xRcPtr)
    ('sqlite3_stmt*', __xRcPtr)
    ('sqlite3_value*', __xRcPtr)
    ('sqlite3_vfs*', __xRcPtr)
    ('void*', __xRcPtr);

    /**
       Populate api object with sqlite3_...() by binding the "raw" wasm
       exports into type-converting proxies using wasm.xWrap().
    */
    for(const e of wasm.bindingSignatures){
      capi[e[0]] = wasm.xWrap.apply(null, e);
    }
    for(const e of wasm.bindingSignatures.wasm){
      wasm[e[0]] = wasm.xWrap.apply(null, e);
    }

    /* For C API functions which cannot work properly unless
       wasm.bigIntEnabled is true, install a bogus impl which throws
       if called when bigIntEnabled is false. The alternative would be
       to elide these functions altogether, which seems likely to
       cause more confusion. */
    const fI64Disabled = function(fname){
      return ()=>toss(fname+"() is unavailable due to lack",
                      "of BigInt support in this build.");
    };
    for(const e of wasm.bindingSignatures.int64){
      capi[e[0]] = wasm.bigIntEnabled
        ? wasm.xWrap.apply(null, e)
        : fI64Disabled(e[0]);
    }

    /* There's no need to expose bindingSignatures to clients,
       implicitly making it part of the public interface. */
    delete wasm.bindingSignatures;

    if(wasm.exports.sqlite3_wasm_db_error){
      const __db_err = wasm.xWrap(
        'sqlite3_wasm_db_error', 'int', 'sqlite3*', 'int', 'string'
      );
      /**
         Sets the given db's error state. Accepts:

         - (sqlite3*, int code, string msg)
         - (sqlite3*, Error e [,string msg = ''+e])

         If passed a WasmAllocError, the message is ignored and the
         result code is SQLITE_NOMEM. If passed any other Error type,
         the result code defaults to SQLITE_ERROR unless the Error
         object has a resultCode property, in which case that is used
         (e.g. SQLite3Error has that). If passed a non-WasmAllocError
         exception, the message string defaults to theError.message.

         Returns the resulting code. Pass (pDb,0,0) to clear the error
         state.
       */
      util.sqlite3_wasm_db_error = function(pDb, resultCode, message){
        if(resultCode instanceof sqlite3.WasmAllocError){
          resultCode = capi.SQLITE_NOMEM;
          message = 0 /*avoid allocating message string*/;
        }else if(resultCode instanceof Error){
          message = message || ''+resultCode;
          resultCode = (resultCode.resultCode || capi.SQLITE_ERROR);
        }
        return pDb ? __db_err(pDb, resultCode, message) : resultCode;
      };
    }else{
      util.sqlite3_wasm_db_error = function(pDb,errCode,msg){
        console.warn("sqlite3_wasm_db_error() is not exported.",arguments);
        return errCode;
      };
    }
  }/*xWrap() bindings*/

  {/* Import C-level constants and structs... */
    const cJson = wasm.xCall('sqlite3_wasm_enum_json');
    if(!cJson){
      toss("Maintenance required: increase sqlite3_wasm_enum_json()'s",
           "static buffer size!");
    }
    //console.debug('wasm.ctype length =',wasm.cstrlen(cJson));
    wasm.ctype = JSON.parse(wasm.cstrToJs(cJson));
    // Groups of SQLITE_xyz macros...
    const defineGroups = ['access', 'authorizer',
                          'blobFinalizers', 'changeset',
                          'config', 'dataTypes',
                          'dbConfig', 'dbStatus',
                          'encodings', 'fcntl', 'flock', 'ioCap',
                          'limits', 'openFlags',
                          'prepareFlags', 'resultCodes',
                          'sqlite3Status',
                          'stmtStatus', 'syncFlags',
                          'trace', 'txnState', 'udfFlags',
                          'version' ];
    if(wasm.bigIntEnabled){
      defineGroups.push('serialize', 'session', 'vtab');
    }
    for(const t of defineGroups){
      for(const e of Object.entries(wasm.ctype[t])){
        // ^^^ [k,v] there triggers a buggy code transformation via
        // one of the Emscripten-driven optimizers.
        capi[e[0]] = e[1];
      }
    }
    if(!wasm.functionEntry(capi.SQLITE_WASM_DEALLOC)){
      toss("Internal error: cannot resolve exported function",
           "entry SQLITE_WASM_DEALLOC (=="+capi.SQLITE_WASM_DEALLOC+").");
    }
    const __rcMap = Object.create(null);
    for(const t of ['resultCodes']){
      for(const e of Object.entries(wasm.ctype[t])){
        __rcMap[e[1]] = e[0];
      }
    }
    /**
       For the given integer, returns the SQLITE_xxx result code as a
       string, or undefined if no such mapping is found.
    */
    capi.sqlite3_js_rc_str = (rc)=>__rcMap[rc];
    /* Bind all registered C-side structs... */
    const notThese = Object.assign(Object.create(null),{
      // For each struct to NOT register, map its name to true:
      WasmTestStruct: true,
      /* We unregister the kvvfs VFS from Worker threads below. */
      sqlite3_kvvfs_methods: !util.isUIThread(),
      /* sqlite3_index_info and friends require int64: */
      sqlite3_index_info: !wasm.bigIntEnabled,
      sqlite3_index_constraint: !wasm.bigIntEnabled,
      sqlite3_index_orderby: !wasm.bigIntEnabled,
      sqlite3_index_constraint_usage: !wasm.bigIntEnabled
    });
    for(const s of wasm.ctype.structs){
      if(!notThese[s.name]){
        capi[s.name] = sqlite3.StructBinder(s);
      }
    }
    if(capi.sqlite3_index_info){
      /* Move these inner structs into sqlite3_index_info.  Binding
      ** them to WASM requires that we create global-scope structs to
      ** model them with, but those are no longer needed after we've
      ** passed them to StructBinder. */
      for(const k of ['sqlite3_index_constraint',
                      'sqlite3_index_orderby',
                      'sqlite3_index_constraint_usage']){
        capi.sqlite3_index_info[k] = capi[k];
        delete capi[k];
      }
      capi.sqlite3_vtab_config = wasm.xWrap(
        'sqlite3_wasm_vtab_config','int',[
          'sqlite3*', 'int', 'int']
      );
    }/* end vtab-related setup */
  }/*end C constant and struct imports*/

  /**
     Internal helper to assist in validating call argument counts in
     the hand-written sqlite3_xyz() wrappers. We do this only for
     consistency with non-special-case wrappings.
  */
  const __dbArgcMismatch = (pDb,f,n)=>{
    return sqlite3.util.sqlite3_wasm_db_error(pDb, capi.SQLITE_MISUSE,
                                              f+"() requires "+n+" argument"+
                                              (1===n?"":'s')+".");
  };

  /** Code duplication reducer for functions which take an encoding
      argument and require SQLITE_UTF8.  Sets the db error code to
      SQLITE_FORMAT and returns that code. */
  const __errEncoding = (pDb)=>{
    return util.sqlite3_wasm_db_error(
      pDb, capi.SQLITE_FORMAT, "SQLITE_UTF8 is the only supported encoding."
    );
  };

  /**
     __dbCleanupMap is infrastructure for recording registration of
     UDFs and collations so that sqlite3_close_v2() can clean up any
     automated JS-to-WASM function conversions installed by those.
  */
  const __argPDb = (pDb)=>wasm.xWrap.argAdapter('sqlite3*')(pDb);
  const __argStr = (str)=>wasm.isPtr(str) ? wasm.cstrToJs(str) : str;
  const __dbCleanupMap = function(
    pDb, mode/*0=remove, >0=create if needed, <0=do not create if missing*/
  ){
    pDb = __argPDb(pDb);
    let m = this.dbMap.get(pDb);
    if(!mode){
      this.dbMap.delete(pDb);
      return m;
    }else if(!m && mode>0){
      this.dbMap.set(pDb, (m = Object.create(null)));
    }
    return m;
  }.bind(Object.assign(Object.create(null),{
    dbMap: new Map
  }));

  __dbCleanupMap.addCollation = function(pDb, name){
    const m = __dbCleanupMap(pDb, 1);
    if(!m.collation) m.collation = new Set;
    m.collation.add(__argStr(name).toLowerCase());
  };

  __dbCleanupMap._addUDF = function(pDb, name, arity, map){
    /* Map UDF name to a Set of arity values */
    name = __argStr(name).toLowerCase();
    let u = map.get(name);
    if(!u) map.set(name, (u = new Set));
    u.add((arity<0) ? -1 : arity);
  };

  __dbCleanupMap.addFunction = function(pDb, name, arity){
    const m = __dbCleanupMap(pDb, 1);
    if(!m.udf) m.udf = new Map;
    this._addUDF(pDb, name, arity, m.udf);
  };

  __dbCleanupMap.addWindowFunc = function(pDb, name, arity){
    const m = __dbCleanupMap(pDb, 1);
    if(!m.wudf) m.wudf = new Map;
    this._addUDF(pDb, name, arity, m.wudf);
  };

  /**
     Intended to be called _only_ from sqlite3_close_v2(),
     passed its non-0 db argument.

     This function frees up certain automatically-installed WASM
     function bindings which were installed on behalf of the given db,
     as those may otherwise leak.

     Notable caveat: this is only ever run via
     sqlite3.capi.sqlite3_close_v2(). If a client, for whatever
     reason, uses sqlite3.wasm.exports.sqlite3_close_v2() (the
     function directly exported from WASM), this cleanup will not
     happen.

     This is not a silver bullet for avoiding automation-related
     leaks but represents "an honest effort."

     The issue being addressed here is covered at:

     https://sqlite.org/wasm/doc/trunk/api-c-style.md#convert-func-ptr
  */
  __dbCleanupMap.cleanup = function(pDb){
    pDb = __argPDb(pDb);
    //wasm.xWrap.FuncPtrAdapter.debugFuncInstall = false;
    /**
       Installing NULL functions in the C API will remove those
       bindings. The FuncPtrAdapter which sits between us and the C
       API will also treat that as an opportunity to
       wasm.uninstallFunction() any WASM function bindings it has
       installed for pDb.
    */
    const closeArgs = [pDb];
    for(const name of [
      'sqlite3_busy_handler',
      'sqlite3_commit_hook',
      'sqlite3_preupdate_hook',
      'sqlite3_progress_handler',
      'sqlite3_rollback_hook',
      'sqlite3_set_authorizer',
      'sqlite3_trace_v2',
      'sqlite3_update_hook'
    ]) {
      const x = wasm.exports[name];
      closeArgs.length = x.length/*==argument count*/
      /* recall that undefined entries translate to 0 when passed to
         WASM. */;
      try{ capi[name](...closeArgs) }
      catch(e){
        console.warn("close-time call of",name+"(",closeArgs,") threw:",e);
      }
    }
    const m = __dbCleanupMap(pDb, 0);
    if(!m) return;
    if(m.collation){
      for(const name of m.collation){
        try{
          capi.sqlite3_create_collation_v2(
            pDb, name, capi.SQLITE_UTF8, 0, 0, 0
          );
        }catch(e){
          /*ignored*/
        }
      }
      delete m.collation;
    }
    let i;
    for(i = 0; i < 2; ++i){ /* Clean up UDFs... */
      const fmap = i ? m.wudf : m.udf;
      if(!fmap) continue;
      const func = i
            ? capi.sqlite3_create_window_function
            : capi.sqlite3_create_function_v2;
      for(const e of fmap){
        const name = e[0], arities = e[1];
        const fargs = [pDb, name, 0/*arity*/, capi.SQLITE_UTF8, 0, 0, 0, 0, 0];
        if(i) fargs.push(0);
        for(const arity of arities){
          try{ fargs[2] = arity; func.apply(null, fargs); }
          catch(e){/*ignored*/}
        }
        arities.clear();
      }
      fmap.clear();
    }
    delete m.udf;
    delete m.wudf;
  }/*__dbCleanupMap.cleanup()*/;

  {/* Binding of sqlite3_close_v2() */
    const __sqlite3CloseV2 = wasm.xWrap("sqlite3_close_v2", "int", "sqlite3*");
    capi.sqlite3_close_v2 = function(pDb){
      if(1!==arguments.length) return __dbArgcMismatch(pDb, 'sqlite3_close_v2', 1);
      if(pDb){
        try{__dbCleanupMap.cleanup(pDb)} catch(e){/*ignored*/}
      }
      return __sqlite3CloseV2(pDb);
    };
  }/*sqlite3_close_v2()*/

  if(capi.sqlite3session_table_filter){
    const __sqlite3SessionDelete = wasm.xWrap(
      'sqlite3session_delete', undefined, ['sqlite3_session*']
    );
    capi.sqlite3session_delete = function(pSession){
      if(1!==arguments.length){
        return __dbArgcMismatch(pDb, 'sqlite3session_delete', 1);
        /* Yes, we're returning a value from a void function. That seems
           like the lesser evil compared to not maintaining arg-count
           consistency as we do with other similar bindings. */
      }
      else if(pSession){
        //wasm.xWrap.FuncPtrAdapter.debugFuncInstall = true;
        capi.sqlite3session_table_filter(pSession, 0, 0);
      }
      __sqlite3SessionDelete(pSession);
    };
  }

  {/* Bindings for sqlite3_create_collation[_v2]() */
    // contextKey() impl for wasm.xWrap.FuncPtrAdapter
    const contextKey = (argv,argIndex)=>{
      return 'argv['+argIndex+']:'+argv[0/* sqlite3* */]+
        ':'+wasm.cstrToJs(argv[1/* collation name */]).toLowerCase()
    };
    const __sqlite3CreateCollationV2 = wasm.xWrap(
      'sqlite3_create_collation_v2', 'int', [
        'sqlite3*', 'string', 'int', '*',
        new wasm.xWrap.FuncPtrAdapter({
          /* int(*xCompare)(void*,int,const void*,int,const void*) */
          name: 'xCompare', signature: 'i(pipip)', contextKey
        }),
        new wasm.xWrap.FuncPtrAdapter({
          /* void(*xDestroy(void*) */
          name: 'xDestroy', signature: 'v(p)', contextKey
        })
      ]
    );

    /**
       Works exactly like C's sqlite3_create_collation_v2() except that:

       1) It returns capi.SQLITE_FORMAT if the 3rd argument contains
          any encoding-related value other than capi.SQLITE_UTF8.  No
          other encodings are supported. As a special case, if the
          bottom 4 bits of that argument are 0, SQLITE_UTF8 is
          assumed.

       2) It accepts JS functions for its function-pointer arguments,
          for which it will install WASM-bound proxies. The bindings
          are "permanent," in that they will stay in the WASM environment
          until it shuts down unless the client calls this again with the
          same collation name and a value of 0 or null for the
          the function pointer(s).

       For consistency with the C API, it requires the same number of
       arguments. It returns capi.SQLITE_MISUSE if passed any other
       argument count.

       Returns 0 on success, non-0 on error, in which case the error
       state of pDb (of type `sqlite3*` or argument-convertible to it)
       may contain more information.
    */
    capi.sqlite3_create_collation_v2 = function(pDb,zName,eTextRep,pArg,xCompare,xDestroy){
      if(6!==arguments.length) return __dbArgcMismatch(pDb, 'sqlite3_create_collation_v2', 6);
      else if( 0 === (eTextRep & 0xf) ){
        eTextRep |= capi.SQLITE_UTF8;
      }else if( capi.SQLITE_UTF8 !== (eTextRep & 0xf) ){
        return __errEncoding(pDb);
      }
      try{
        const rc = __sqlite3CreateCollationV2(pDb, zName, eTextRep, pArg, xCompare, xDestroy);
        if(0===rc && xCompare instanceof Function){
          __dbCleanupMap.addCollation(pDb, zName);
        }
        return rc;
      }catch(e){
        return util.sqlite3_wasm_db_error(pDb, e);
      }
    };

    capi.sqlite3_create_collation = (pDb,zName,eTextRep,pArg,xCompare)=>{
      return (5===arguments.length)
        ? capi.sqlite3_create_collation_v2(pDb,zName,eTextRep,pArg,xCompare,0)
        : __dbArgcMismatch(pDb, 'sqlite3_create_collation', 5);
    };

  }/*sqlite3_create_collation() and friends*/

  {/* Special-case handling of sqlite3_create_function_v2()
      and sqlite3_create_window_function(). */
    /** FuncPtrAdapter for contextKey() for sqlite3_create_function()
        and friends. */
    const contextKey = function(argv,argIndex){
      return (
        argv[0/* sqlite3* */]
          +':'+(argv[2/*number of UDF args*/] < 0 ? -1 : argv[2])
          +':'+argIndex/*distinct for each xAbc callback type*/
          +':'+wasm.cstrToJs(argv[1]).toLowerCase()
      )
    };

    /**
       JS proxies for the various sqlite3_create[_window]_function()
       callbacks, structured in a form usable by wasm.xWrap.FuncPtrAdapter.
    */
    const __cfProxy = Object.assign(Object.create(null), {
      xInverseAndStep: {
        signature:'v(pip)', contextKey,
        callProxy: (callback)=>{
          return (pCtx, argc, pArgv)=>{
            try{ callback(pCtx, ...capi.sqlite3_values_to_js(argc, pArgv)) }
            catch(e){ capi.sqlite3_result_error_js(pCtx, e) }
          };
        }
      },
      xFinalAndValue: {
        signature:'v(p)', contextKey,
        callProxy: (callback)=>{
          return (pCtx)=>{
            try{ capi.sqlite3_result_js(pCtx, callback(pCtx)) }
            catch(e){ capi.sqlite3_result_error_js(pCtx, e) }
          };
        }
      },
      xFunc: {
        signature:'v(pip)', contextKey,
        callProxy: (callback)=>{
          return (pCtx, argc, pArgv)=>{
            try{
              capi.sqlite3_result_js(
                pCtx,
                callback(pCtx, ...capi.sqlite3_values_to_js(argc, pArgv))
              );
            }catch(e){
              //console.error('xFunc() caught:',e);
              capi.sqlite3_result_error_js(pCtx, e);
            }
          };
        }
      },
      xDestroy: {
        signature:'v(p)', contextKey,
        //Arguable: a well-behaved destructor doesn't require a proxy.
        callProxy: (callback)=>{
          return (pVoid)=>{
            try{ callback(pVoid) }
            catch(e){ console.error("UDF xDestroy method threw:",e) }
          };
        }
      }
    })/*__cfProxy*/;

    const __sqlite3CreateFunction = wasm.xWrap(
      "sqlite3_create_function_v2", "int", [
        "sqlite3*", "string"/*funcName*/, "int"/*nArg*/,
        "int"/*eTextRep*/, "*"/*pApp*/,
        new wasm.xWrap.FuncPtrAdapter({name: 'xFunc', ...__cfProxy.xFunc}),
        new wasm.xWrap.FuncPtrAdapter({name: 'xStep', ...__cfProxy.xInverseAndStep}),
        new wasm.xWrap.FuncPtrAdapter({name: 'xFinal', ...__cfProxy.xFinalAndValue}),
        new wasm.xWrap.FuncPtrAdapter({name: 'xDestroy', ...__cfProxy.xDestroy})
      ]
    );

    const __sqlite3CreateWindowFunction = wasm.xWrap(
      "sqlite3_create_window_function", "int", [
        "sqlite3*", "string"/*funcName*/, "int"/*nArg*/,
        "int"/*eTextRep*/, "*"/*pApp*/,
        new wasm.xWrap.FuncPtrAdapter({name: 'xStep', ...__cfProxy.xInverseAndStep}),
        new wasm.xWrap.FuncPtrAdapter({name: 'xFinal', ...__cfProxy.xFinalAndValue}),
        new wasm.xWrap.FuncPtrAdapter({name: 'xValue', ...__cfProxy.xFinalAndValue}),
        new wasm.xWrap.FuncPtrAdapter({name: 'xInverse', ...__cfProxy.xInverseAndStep}),
        new wasm.xWrap.FuncPtrAdapter({name: 'xDestroy', ...__cfProxy.xDestroy})
      ]
    );

    /* Documented in the api object's initializer. */
    capi.sqlite3_create_function_v2 = function f(
      pDb, funcName, nArg, eTextRep, pApp,
      xFunc,   //void (*xFunc)(sqlite3_context*,int,sqlite3_value**)
      xStep,   //void (*xStep)(sqlite3_context*,int,sqlite3_value**)
      xFinal,  //void (*xFinal)(sqlite3_context*)
      xDestroy //void (*xDestroy)(void*)
    ){
      if( f.length!==arguments.length ){
        return __dbArgcMismatch(pDb,"sqlite3_create_function_v2",f.length);
      }else if( 0 === (eTextRep & 0xf) ){
        eTextRep |= capi.SQLITE_UTF8;
      }else if( capi.SQLITE_UTF8 !== (eTextRep & 0xf) ){
        return __errEncoding(pDb);
      }
      try{
        const rc = __sqlite3CreateFunction(pDb, funcName, nArg, eTextRep,
                                           pApp, xFunc, xStep, xFinal, xDestroy);
        if(0===rc && (xFunc instanceof Function
                      || xStep instanceof Function
                      || xFinal instanceof Function
                      || xDestroy instanceof Function)){
          __dbCleanupMap.addFunction(pDb, funcName, nArg);
        }
        return rc;
      }catch(e){
        console.error("sqlite3_create_function_v2() setup threw:",e);
        return util.sqlite3_wasm_db_error(pDb, e, "Creation of UDF threw: "+e);
      }
    };

    /* Documented in the api object's initializer. */
    capi.sqlite3_create_function = function f(
      pDb, funcName, nArg, eTextRep, pApp,
      xFunc, xStep, xFinal
    ){
      return (f.length===arguments.length)
        ? capi.sqlite3_create_function_v2(pDb, funcName, nArg, eTextRep,
                                          pApp, xFunc, xStep, xFinal, 0)
        : __dbArgcMismatch(pDb,"sqlite3_create_function",f.length);
    };

    /* Documented in the api object's initializer. */
    capi.sqlite3_create_window_function = function f(
      pDb, funcName, nArg, eTextRep, pApp,
      xStep,   //void (*xStep)(sqlite3_context*,int,sqlite3_value**)
      xFinal,  //void (*xFinal)(sqlite3_context*)
      xValue,  //void (*xValue)(sqlite3_context*)
      xInverse,//void (*xInverse)(sqlite3_context*,int,sqlite3_value**)
      xDestroy //void (*xDestroy)(void*)
    ){
      if( f.length!==arguments.length ){
        return __dbArgcMismatch(pDb,"sqlite3_create_window_function",f.length);
      }else if( 0 === (eTextRep & 0xf) ){
        eTextRep |= capi.SQLITE_UTF8;
      }else if( capi.SQLITE_UTF8 !== (eTextRep & 0xf) ){
        return __errEncoding(pDb);
      }
      try{
        const rc = __sqlite3CreateWindowFunction(pDb, funcName, nArg, eTextRep,
                                                 pApp, xStep, xFinal, xValue,
                                                 xInverse, xDestroy);
        if(0===rc && (xStep instanceof Function
                      || xFinal instanceof Function
                      || xValue instanceof Function
                      || xInverse instanceof Function
                      || xDestroy instanceof Function)){
          __dbCleanupMap.addWindowFunc(pDb, funcName, nArg);
        }
        return rc;
      }catch(e){
        console.error("sqlite3_create_window_function() setup threw:",e);
        return util.sqlite3_wasm_db_error(pDb, e, "Creation of UDF threw: "+e);
      }
    };
    /**
       A _deprecated_ alias for capi.sqlite3_result_js() which
       predates the addition of that function in the public API.
    */
    capi.sqlite3_create_function_v2.udfSetResult =
      capi.sqlite3_create_function.udfSetResult =
      capi.sqlite3_create_window_function.udfSetResult = capi.sqlite3_result_js;

    /**
       A _deprecated_ alias for capi.sqlite3_values_to_js() which
       predates the addition of that function in the public API.
    */
    capi.sqlite3_create_function_v2.udfConvertArgs =
      capi.sqlite3_create_function.udfConvertArgs =
      capi.sqlite3_create_window_function.udfConvertArgs = capi.sqlite3_values_to_js;

    /**
       A _deprecated_ alias for capi.sqlite3_result_error_js() which
       predates the addition of that function in the public API.
    */
    capi.sqlite3_create_function_v2.udfSetError =
      capi.sqlite3_create_function.udfSetError =
      capi.sqlite3_create_window_function.udfSetError = capi.sqlite3_result_error_js;

  }/*sqlite3_create_function_v2() and sqlite3_create_window_function() proxies*/;

  {/* Special-case handling of sqlite3_prepare_v2() and
      sqlite3_prepare_v3() */

    /**
       Helper for string:flexible conversions which require a
       byte-length counterpart argument. Passed a value and its
       ostensible length, this function returns [V,N], where V is
       either v or a transformed copy of v and N is either n, -1, or
       the byte length of v (if it's a byte array or ArrayBuffer).
    */
    const __flexiString = (v,n)=>{
      if('string'===typeof v){
        n = -1;
      }else if(util.isSQLableTypedArray(v)){
        n = v.byteLength;
        v = util.typedArrayToString(
          (v instanceof ArrayBuffer) ? new Uint8Array(v) : v
        );
      }else if(Array.isArray(v)){
        v = v.join("");
        n = -1;
      }
      return [v, n];
    };

    /**
       Scope-local holder of the two impls of sqlite3_prepare_v2/v3().
    */
    const __prepare = {
      /**
         This binding expects a JS string as its 2nd argument and
         null as its final argument. In order to compile multiple
         statements from a single string, the "full" impl (see
         below) must be used.
      */
      basic: wasm.xWrap('sqlite3_prepare_v3',
                        "int", ["sqlite3*", "string",
                                "int"/*ignored for this impl!*/,
                                "int", "**",
                                "**"/*MUST be 0 or null or undefined!*/]),
      /**
         Impl which requires that the 2nd argument be a pointer
         to the SQL string, instead of being converted to a
         string. This variant is necessary for cases where we
         require a non-NULL value for the final argument
         (exec()'ing multiple statements from one input
         string). For simpler cases, where only the first
         statement in the SQL string is required, the wrapper
         named sqlite3_prepare_v2() is sufficient and easier to
         use because it doesn't require dealing with pointers.
      */
      full: wasm.xWrap('sqlite3_prepare_v3',
                       "int", ["sqlite3*", "*", "int", "int",
                               "**", "**"])
    };

    /* Documented in the capi object's initializer. */
    capi.sqlite3_prepare_v3 = function f(pDb, sql, sqlLen, prepFlags, ppStmt, pzTail){
      if(f.length!==arguments.length){
        return __dbArgcMismatch(pDb,"sqlite3_prepare_v3",f.length);
      }
      const [xSql, xSqlLen] = __flexiString(sql, sqlLen);
      switch(typeof xSql){
          case 'string': return __prepare.basic(pDb, xSql, xSqlLen, prepFlags, ppStmt, null);
          case 'number': return __prepare.full(pDb, xSql, xSqlLen, prepFlags, ppStmt, pzTail);
          default:
            return util.sqlite3_wasm_db_error(
              pDb, capi.SQLITE_MISUSE,
              "Invalid SQL argument type for sqlite3_prepare_v2/v3()."
            );
      }
    };

    /* Documented in the capi object's initializer. */
    capi.sqlite3_prepare_v2 = function f(pDb, sql, sqlLen, ppStmt, pzTail){
      return (f.length===arguments.length)
        ? capi.sqlite3_prepare_v3(pDb, sql, sqlLen, 0, ppStmt, pzTail)
        : __dbArgcMismatch(pDb,"sqlite3_prepare_v2",f.length);
    };

  }/*sqlite3_prepare_v2/v3()*/

  {/*sqlite3_bind_text/blob()*/
    const __bindText = wasm.xWrap("sqlite3_bind_text", "int", [
      "sqlite3_stmt*", "int", "string", "int", "*"
    ]);
    const __bindBlob = wasm.xWrap("sqlite3_bind_blob", "int", [
      "sqlite3_stmt*", "int", "*", "int", "*"
    ]);

    /** Documented in the capi object's initializer. */
    capi.sqlite3_bind_text = function f(pStmt, iCol, text, nText, xDestroy){
      if(f.length!==arguments.length){
        return __dbArgcMismatch(capi.sqlite3_db_handle(pStmt),
                                "sqlite3_bind_text", f.length);
      }else if(wasm.isPtr(text) || null===text){
        return __bindText(pStmt, iCol, text, nText, xDestroy);
      }else if(text instanceof ArrayBuffer){
        text = new Uint8Array(text);
      }else if(Array.isArray(pMem)){
        text = pMem.join('');
      }
      let p, n;
      try{
        if(util.isSQLableTypedArray(text)){
          p = wasm.allocFromTypedArray(text);
          n = text.byteLength;
        }else if('string'===typeof text){
          [p, n] = wasm.allocCString(text);
        }else{
          return util.sqlite3_wasm_db_error(
            capi.sqlite3_db_handle(pStmt), capi.SQLITE_MISUSE,
            "Invalid 3rd argument type for sqlite3_bind_text()."
          );
        }
        return __bindText(pStmt, iCol, p, n, capi.SQLITE_WASM_DEALLOC);
      }catch(e){
        wasm.dealloc(p);
        return util.sqlite3_wasm_db_error(
          capi.sqlite3_db_handle(pStmt), e
        );
      }
    }/*sqlite3_bind_text()*/;

    /** Documented in the capi object's initializer. */
    capi.sqlite3_bind_blob = function f(pStmt, iCol, pMem, nMem, xDestroy){
      if(f.length!==arguments.length){
        return __dbArgcMismatch(capi.sqlite3_db_handle(pStmt),
                                "sqlite3_bind_blob", f.length);
      }else if(wasm.isPtr(pMem) || null===pMem){
        return __bindBlob(pStmt, iCol, pMem, nMem, xDestroy);
      }else if(pMem instanceof ArrayBuffer){
        pMem = new Uint8Array(pMem);
      }else if(Array.isArray(pMem)){
        pMem = pMem.join('');
      }
      let p, n;
      try{
        if(util.isBindableTypedArray(pMem)){
          p = wasm.allocFromTypedArray(pMem);
          n = nMem>=0 ? nMem : pMem.byteLength;
        }else if('string'===typeof pMem){
          [p, n] = wasm.allocCString(pMem);
        }else{
          return util.sqlite3_wasm_db_error(
            capi.sqlite3_db_handle(pStmt), capi.SQLITE_MISUSE,
            "Invalid 3rd argument type for sqlite3_bind_blob()."
          );
        }
        return __bindBlob(pStmt, iCol, p, n, capi.SQLITE_WASM_DEALLOC);
      }catch(e){
        wasm.dealloc(p);
        return util.sqlite3_wasm_db_error(
          capi.sqlite3_db_handle(pStmt), e
        );
      }
    }/*sqlite3_bind_blob()*/;

  }/*sqlite3_bind_text/blob()*/

  {/* sqlite3_config() */
    /**
       Wraps a small subset of the C API's sqlite3_config() options.
       Unsupported options trigger the return of capi.SQLITE_NOTFOUND.
       Passing fewer than 2 arguments triggers return of
       capi.SQLITE_MISUSE.
    */
    capi.sqlite3_config = function(op, ...args){
      if(arguments.length<2) return capi.SQLITE_MISUSE;
      switch(op){
          case capi.SQLITE_CONFIG_COVERING_INDEX_SCAN: // 20  /* int */
          case capi.SQLITE_CONFIG_MEMSTATUS:// 9  /* boolean */
          case capi.SQLITE_CONFIG_SMALL_MALLOC: // 27  /* boolean */
          case capi.SQLITE_CONFIG_SORTERREF_SIZE: // 28  /* int nByte */
          case capi.SQLITE_CONFIG_STMTJRNL_SPILL: // 26  /* int nByte */
          case capi.SQLITE_CONFIG_URI:// 17  /* int */
            return wasm.exports.sqlite3_wasm_config_i(op, args[0]);
          case capi.SQLITE_CONFIG_LOOKASIDE: // 13  /* int int */
            return wasm.exports.sqlite3_wasm_config_ii(op, args[0], args[1]);
          case capi.SQLITE_CONFIG_MEMDB_MAXSIZE: // 29  /* sqlite3_int64 */
            return wasm.exports.sqlite3_wasm_config_j(op, args[0]);
          case capi.SQLITE_CONFIG_GETMALLOC: // 5 /* sqlite3_mem_methods* */
          case capi.SQLITE_CONFIG_GETMUTEX: // 11  /* sqlite3_mutex_methods* */
          case capi.SQLITE_CONFIG_GETPCACHE2: // 19  /* sqlite3_pcache_methods2* */
          case capi.SQLITE_CONFIG_GETPCACHE: // 15  /* no-op */
          case capi.SQLITE_CONFIG_HEAP: // 8  /* void*, int nByte, int min */
          case capi.SQLITE_CONFIG_LOG: // 16  /* xFunc, void* */
          case capi.SQLITE_CONFIG_MALLOC:// 4  /* sqlite3_mem_methods* */
          case capi.SQLITE_CONFIG_MMAP_SIZE: // 22  /* sqlite3_int64, sqlite3_int64 */
          case capi.SQLITE_CONFIG_MULTITHREAD: // 2 /* nil */
          case capi.SQLITE_CONFIG_MUTEX: // 10  /* sqlite3_mutex_methods* */
          case capi.SQLITE_CONFIG_PAGECACHE: // 7  /* void*, int sz, int N */
          case capi.SQLITE_CONFIG_PCACHE2: // 18  /* sqlite3_pcache_methods2* */
          case capi.SQLITE_CONFIG_PCACHE: // 14  /* no-op */
          case capi.SQLITE_CONFIG_PCACHE_HDRSZ: // 24  /* int *psz */
          case capi.SQLITE_CONFIG_PMASZ: // 25  /* unsigned int szPma */
          case capi.SQLITE_CONFIG_SERIALIZED: // 3 /* nil */
          case capi.SQLITE_CONFIG_SINGLETHREAD: // 1 /* nil */:
          case capi.SQLITE_CONFIG_SQLLOG: // 21  /* xSqllog, void* */
          case capi.SQLITE_CONFIG_WIN32_HEAPSIZE: // 23  /* int nByte */
          default:
            return capi.SQLITE_NOTFOUND;
      }
    };
  }/* sqlite3_config() */

  {/*auto-extension bindings.*/
    const __autoExtFptr = new Set;

    capi.sqlite3_auto_extension = function(fPtr){
      if( fPtr instanceof Function ){
        fPtr = wasm.installFunction('i(ppp)', fPtr);
      }else if( 1!==arguments.length || !wasm.isPtr(fPtr) ){
        return capi.SQLITE_MISUSE;
      }
      const rc = wasm.exports.sqlite3_auto_extension(fPtr);
      if( fPtr!==arguments[0] ){
        if(0===rc) __autoExtFptr.add(fPtr);
        else wasm.uninstallFunction(fPtr);
      }
      return rc;
    };

    capi.sqlite3_cancel_auto_extension = function(fPtr){
     /* We do not do an automatic JS-to-WASM function conversion here
        because it would be senseless: the converted pointer would
        never possibly match an already-installed one. */;
      if(!fPtr || 1!==arguments.length || !wasm.isPtr(fPtr)) return 0;
      return wasm.exports.sqlite3_cancel_auto_extension(fPtr);
      /* Note that it "cannot happen" that a client passes a pointer which
         is in __autoExtFptr because __autoExtFptr only contains automatic
         conversions created inside sqlite3_auto_extension() and
         never exposed to the client. */
    };

    capi.sqlite3_reset_auto_extension = function(){
      wasm.exports.sqlite3_reset_auto_extension();
      for(const fp of __autoExtFptr) wasm.uninstallFunction(fp);
      __autoExtFptr.clear();
    };
  }/* auto-extension */

  const pKvvfs = capi.sqlite3_vfs_find("kvvfs");
  if( pKvvfs ){/* kvvfs-specific glue */
    if(util.isUIThread()){
      const kvvfsMethods = new capi.sqlite3_kvvfs_methods(
        wasm.exports.sqlite3_wasm_kvvfs_methods()
      );
      delete capi.sqlite3_kvvfs_methods;

      const kvvfsMakeKey = wasm.exports.sqlite3_wasm_kvvfsMakeKeyOnPstack,
            pstack = wasm.pstack;

      const kvvfsStorage = (zClass)=>
            ((115/*=='s'*/===wasm.peek(zClass))
             ? sessionStorage : localStorage);

      /**
         Implementations for members of the object referred to by
         sqlite3_wasm_kvvfs_methods(). We swap out the native
         implementations with these, which use localStorage or
         sessionStorage for their backing store.
      */
      const kvvfsImpls = {
        xRead: (zClass, zKey, zBuf, nBuf)=>{
          const stack = pstack.pointer,
                astack = wasm.scopedAllocPush();
          try {
            const zXKey = kvvfsMakeKey(zClass,zKey);
            if(!zXKey) return -3/*OOM*/;
            const jKey = wasm.cstrToJs(zXKey);
            const jV = kvvfsStorage(zClass).getItem(jKey);
            if(!jV) return -1;
            const nV = jV.length /* Note that we are relying 100% on v being
                                    ASCII so that jV.length is equal to the
                                    C-string's byte length. */;
            if(nBuf<=0) return nV;
            else if(1===nBuf){
              wasm.poke(zBuf, 0);
              return nV;
            }
            const zV = wasm.scopedAllocCString(jV);
            if(nBuf > nV + 1) nBuf = nV + 1;
            wasm.heap8u().copyWithin(zBuf, zV, zV + nBuf - 1);
            wasm.poke(zBuf + nBuf - 1, 0);
            return nBuf - 1;
          }catch(e){
            console.error("kvstorageRead()",e);
            return -2;
          }finally{
            pstack.restore(stack);
            wasm.scopedAllocPop(astack);
          }
        },
        xWrite: (zClass, zKey, zData)=>{
          const stack = pstack.pointer;
          try {
            const zXKey = kvvfsMakeKey(zClass,zKey);
            if(!zXKey) return 1/*OOM*/;
            const jKey = wasm.cstrToJs(zXKey);
            kvvfsStorage(zClass).setItem(jKey, wasm.cstrToJs(zData));
            return 0;
          }catch(e){
            console.error("kvstorageWrite()",e);
            return capi.SQLITE_IOERR;
          }finally{
            pstack.restore(stack);
          }
        },
        xDelete: (zClass, zKey)=>{
          const stack = pstack.pointer;
          try {
            const zXKey = kvvfsMakeKey(zClass,zKey);
            if(!zXKey) return 1/*OOM*/;
            kvvfsStorage(zClass).removeItem(wasm.cstrToJs(zXKey));
            return 0;
          }catch(e){
            console.error("kvstorageDelete()",e);
            return capi.SQLITE_IOERR;
          }finally{
            pstack.restore(stack);
          }
        }
      }/*kvvfsImpls*/;
      for(const k of Object.keys(kvvfsImpls)){
        kvvfsMethods[kvvfsMethods.memberKey(k)] =
          wasm.installFunction(
            kvvfsMethods.memberSignature(k),
            kvvfsImpls[k]
          );
      }
    }else{
      /* Worker thread: unregister kvvfs to avoid it being used
         for anything other than local/sessionStorage. It "can"
         be used that way but it's not really intended to be. */
      capi.sqlite3_vfs_unregister(pKvvfs);
    }
  }/*pKvvfs*/

  wasm.xWrap.FuncPtrAdapter.warnOnUse = true;
});
