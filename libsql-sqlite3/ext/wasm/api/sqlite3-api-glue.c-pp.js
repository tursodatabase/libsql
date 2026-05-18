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
  (e.g. sqlite3-api-oo1.js) have all of the infrastructure that they
  need.
*/
globalThis.sqlite3ApiBootstrap.initializers.push(function(sqlite3){
  'use strict';
  const toss = (...args)=>{throw new Error(args.join(' '))};
  const capi = sqlite3.capi, wasm = sqlite3.wasm, util = sqlite3.util;
  globalThis.WhWasmUtilInstaller(wasm);
  delete globalThis.WhWasmUtilInstaller;

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

     Support for the API-specific data types in the result/argument
     type strings gets plugged in at a later phase in the API
     initialization process.
  */
  const bindingSignatures = {
    core: [
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
      ["sqlite3_bind_parameter_name", "string", "sqlite3_stmt*", "int"],
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
      ["sqlite3_column_decltype", "string", "sqlite3_stmt*", "int"],
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
      /* sqlite3_create_collation() and sqlite3_create_collation_v2()
         use hand-written bindings to simplify passing of the callback
         function. */
      /* sqlite3_create_function(), sqlite3_create_function_v2(), and
         sqlite3_create_window_function() use hand-written bindings to
         simplify handling of their function-type arguments. */
      ["sqlite3_data_count", "int", "sqlite3_stmt*"],
      ["sqlite3_db_filename", "string", "sqlite3*", "string"],
      ["sqlite3_db_handle", "sqlite3*", "sqlite3_stmt*"],
      ["sqlite3_db_name", "string", "sqlite3*", "int"],
      ["sqlite3_db_readonly", "int", "sqlite3*", "string"],
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
      ["sqlite3_get_autocommit", "int", "sqlite3*"],
      ["sqlite3_get_auxdata", "*", "sqlite3_context*", "int"],
      ["sqlite3_initialize", undefined],
      ["sqlite3_interrupt", undefined, "sqlite3*"],
      ["sqlite3_is_interrupted", "int", "sqlite3*"],
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
      /**
         We do not have a way to automatically clean up destructors
         which are automatically converted from JS functions via the
         final argument to sqlite3_set_auxdata(). Because of that,
         automatic function conversion is not supported for this
         function.  Clients should use wasm.installFunction() to create
         such callbacks, then pass that pointer to
         sqlite3_set_auxdata(). Relying on automated conversions here
         would lead to leaks of JS/WASM proxy functions because
         sqlite3_set_auxdata() is frequently called in UDFs.

         The sqlite3.oo1.DB class's onclose handlers can be used for this
         purpose. For example:

         const pAuxDtor = wasm.installFunction('v(p)', function(ptr){
           //free ptr
         });
         myDb.onclose = {
           after: ()=>{
             wasm.uninstallFunction(pAuxDtor);
           }
         };

         Then pass pAuxDtor as the final argument to appropriate
         sqlite3_set_auxdata() calls.

         Prior to 3.49.0 this binding ostensibly had automatic
         function conversion here but a typo prevented it from
         working.  Rather than fix it, it was removed because testing
         the fix brought the huge potential for memory leaks to the
         forefront.
      */
      ["sqlite3_set_auxdata", undefined, [
        "sqlite3_context*", "int", "*",
        true
          ? "*"
          : new wasm.xWrap.FuncPtrAdapter({
            /* If we can find a way to automate their cleanup, JS functions can
               be auto-converted with this. */
            name: 'xDestroyAuxData',
            signature: 'v(p)',
            contextKey: (argv, argIndex)=>argv[0/* sqlite3_context* */]
          })
      ]],
      ['sqlite3_set_errmsg', 'int', 'sqlite3*', 'int', 'string'],
      ["sqlite3_shutdown", undefined],
      ["sqlite3_sourceid", "string"],
      ["sqlite3_sql", "string", "sqlite3_stmt*"],
      ["sqlite3_status", "int", "int", "*", "*", "int"],
      ["sqlite3_step", "int", "sqlite3_stmt*"],
      ["sqlite3_stmt_busy", "int", "sqlite3_stmt*"],
      ["sqlite3_stmt_readonly", "int", "sqlite3_stmt*"],
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
      /* sqlite3_uri_...() have very specific requirements for their
         first C-string arguments, so we cannot perform any
         string-type conversion on their first argument. */
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

      /* This list gets extended with optional pieces below */
    ]/*.core*/,
    /**
       Functions which require BigInt (int64) support are separated
       from the others because we need to conditionally bind them or
       apply dummy impls, depending on the capabilities of the
       environment.  (That said: we never actually build without
       BigInt support, and such builds are untested.)

       Not all of these functions directly require int64 but are only
       for use with APIs which require int64. For example, the
       vtab-related functions.
    */
    int64: [
      ["sqlite3_bind_int64","int", ["sqlite3_stmt*", "int", "i64"]],
      ["sqlite3_changes64","i64", ["sqlite3*"]],
      ["sqlite3_column_int64","i64", ["sqlite3_stmt*", "int"]],
      ["sqlite3_deserialize", "int", "sqlite3*", "string", "*", "i64", "i64", "int"]
      /* Careful! Short version: de/serialize() are problematic because they
         might use a different allocator than the user for managing the
         deserialized block. de/serialize() are ONLY safe to use with
         sqlite3_malloc(), sqlite3_free(), and its 64-bit variants. Because
         of this, the canonical builds of sqlite3.wasm/js guarantee that
         sqlite3.wasm.alloc() and friends use those allocators. Custom builds
         may not guarantee that, however. */,
      ["sqlite3_last_insert_rowid", "i64", ["sqlite3*"]],
      ["sqlite3_malloc64", "*","i64"],
      ["sqlite3_msize", "i64", "*"],
      ["sqlite3_overload_function", "int", ["sqlite3*","string","int"]],
      ["sqlite3_realloc64", "*","*", "i64"],
      ["sqlite3_result_int64", undefined, "*", "i64"],
      ["sqlite3_result_zeroblob64", "int", "*", "i64"],
      ["sqlite3_serialize","*", "sqlite3*", "string", "*", "int"],
      ["sqlite3_set_last_insert_rowid", undefined, ["sqlite3*", "i64"]],
      ["sqlite3_status64", "int", "int", "*", "*", "int"],
      ["sqlite3_db_status64", "int", "sqlite3*", "int", "*", "*", "int"],
      ["sqlite3_total_changes64", "i64", ["sqlite3*"]],
      ["sqlite3_update_hook", "*", [
        "sqlite3*",
        new wasm.xWrap.FuncPtrAdapter({
          name: 'sqlite3_update_hook::callback',
          signature: "v(pippj)",
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
      ["sqlite3_value_int64","i64", "sqlite3_value*"]
      /* This list gets extended with optional pieces below */
    ]/*.int64*/,
    /**
       Functions which are intended solely for API-internal use by the
       WASM components, not client code. These get installed into
       sqlite3.util. Some of them get exposed to clients via variants
       in sqlite3_js_...().

       2024-01-11: these were renamed, with two underscores in the
       prefix, to ensure that clients do not accidentally depend on
       them.  They have always been documented as internal-use-only,
       so no clients "should" be depending on the old names.
    */
    wasmInternal: [
      ["sqlite3__wasm_db_reset", "int", "sqlite3*"],
      ["sqlite3__wasm_db_vfs", "sqlite3_vfs*", "sqlite3*","string"],
      [/* DO NOT USE. This is deprecated since 2023-08-11 because it
          can trigger assert() in debug builds when used with file
          sizes which are not an exact multiple of a valid db page
          size.  This function is retained only so that
          sqlite3_js_vfs_create_file() can continue to work (for a
          given value of work), but that function emits a
          config.warn() log message directing the reader to
          alternatives. */
        "sqlite3__wasm_vfs_create_file", "int", "sqlite3_vfs*","string","*", "int"
      ],
      ["sqlite3__wasm_posix_create_file", "int", "string","*", "int"],
      ["sqlite3__wasm_vfs_unlink", "int", "sqlite3_vfs*","string"],
      ["sqlite3__wasm_qfmt_token","string:dealloc", "string","int"]
    ]/*.wasmInternal*/
  } /*bindingSignatures*/;

  if( !!wasm.exports.sqlite3_progress_handler ){
    bindingSignatures.core.push(
      ["sqlite3_progress_handler", undefined, [
        "sqlite3*", "int", new wasm.xWrap.FuncPtrAdapter({
          name: 'xProgressHandler',
          signature: 'i(p)',
          bindScope: 'context',
          contextKey: (argv,argIndex)=>argv[0/* sqlite3* */]
        }), "*"
      ]]
    );
  }

  if( !!wasm.exports.sqlite3_stmt_explain ){
    bindingSignatures.core.push(
      ["sqlite3_stmt_explain", "int", "sqlite3_stmt*", "int"],
      ["sqlite3_stmt_isexplain", "int", "sqlite3_stmt*"]
    );
  }

  if( !!wasm.exports.sqlite3_set_authorizer ){
    bindingSignatures.core.push(
      ["sqlite3_set_authorizer", "int", [
        "sqlite3*",
        new wasm.xWrap.FuncPtrAdapter({
          name: "sqlite3_set_authorizer::xAuth",
          signature: "i(pi"+"ssss)",
          contextKey: (argv, argIndex)=>argv[0/*(sqlite3*)*/],
          /**
             We use callProxy here to ensure (A) that exceptions
             thrown from callback() have well-defined behavior and (B)
             that its result is coerced to an integer.
          */
          callProxy: (callback)=>{
            return (pV, iCode, s0, s1, s2, s3)=>{
              try{
                s0 = s0 && wasm.cstrToJs(s0); s1 = s1 && wasm.cstrToJs(s1);
                s2 = s2 && wasm.cstrToJs(s2); s3 = s3 && wasm.cstrToJs(s3);
                return callback(pV, iCode, s0, s1, s2, s3) | 0;
              }catch(e){
                return e.resultCode || capi.SQLITE_ERROR;
              }
            }
          }
        }),
        "*"/*pUserData*/
      ]]
    );
  }/* sqlite3_set_authorizer() */

  if( !!wasm.exports.sqlite3_column_origin_name ){
    bindingSignatures.core.push(
      ["sqlite3_column_database_name","string", "sqlite3_stmt*", "int"],
      ["sqlite3_column_origin_name","string", "sqlite3_stmt*", "int"],
      ["sqlite3_column_table_name","string", "sqlite3_stmt*", "int"]
    );
  }

  if(false && wasm.compileOptionUsed('SQLITE_ENABLE_NORMALIZE')){
    /* ^^^ "the problem" is that this is an optional feature and the
       build-time function-export list does not currently take
       optional features into account. */
    bindingSignatures.core.push(["sqlite3_normalized_sql", "string", "sqlite3_stmt*"]);
  }

//#if enable-see
  if( !!wasm.exports.sqlite3_key_v2 ){
    /**
       This code is capable of using an SEE build but an SEE WASM
       build is generally incompatible with SEE's license conditions.
       The project's official stance on WASM builds of SEE is: it is
       permitted for use internally within organizations which have
       licensed SEE, but not for sites made available to the public or
       to unlicensed end users because exposing an SEE build of
       sqlite3.wasm effectively provides all clients with a working
       copy of SEE.
    */
    bindingSignatures.core.push(
      ["sqlite3_key", "int", "sqlite3*", "string", "int"],
      ["sqlite3_key_v2","int","sqlite3*","string","*","int"],
      ["sqlite3_rekey", "int", "sqlite3*", "string", "int"],
      ["sqlite3_rekey_v2", "int", "sqlite3*", "string", "*", "int"],
      ["sqlite3_activate_see", undefined, "string"]
    );
  }
//#endif enable-see

  if( wasm.bigIntEnabled && !!wasm.exports.sqlite3_declare_vtab ){
    bindingSignatures.int64.push(
      ["sqlite3_create_module", "int",
       ["sqlite3*","string","sqlite3_module*","*"]],
      ["sqlite3_create_module_v2", "int",
       ["sqlite3*","string","sqlite3_module*","*","*"]],
      ["sqlite3_declare_vtab", "int", ["sqlite3*", "string:flexible"]],
      ["sqlite3_drop_modules", "int", ["sqlite3*", "**"]],
      ["sqlite3_vtab_collation","string","sqlite3_index_info*","int"],
      /*["sqlite3_vtab_config" is variadic and requires a hand-written
        proxy.] */
      ["sqlite3_vtab_distinct","int", "sqlite3_index_info*"],
      ["sqlite3_vtab_in","int", "sqlite3_index_info*", "int", "int"],
      ["sqlite3_vtab_in_first", "int", "sqlite3_value*", "**"],
      ["sqlite3_vtab_in_next", "int", "sqlite3_value*", "**"],
      ["sqlite3_vtab_nochange","int", "sqlite3_context*"],
      ["sqlite3_vtab_on_conflict","int", "sqlite3*"],
      ["sqlite3_vtab_rhs_value","int", "sqlite3_index_info*", "int", "**"]
    );
  }/* virtual table APIs */

  if(wasm.bigIntEnabled && !!wasm.exports.sqlite3_preupdate_hook){
    bindingSignatures.int64.push(
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
      ["sqlite3_preupdate_old", "int", ["sqlite3*", "int", "**"]]
    );
  } /* preupdate API */

  // Add session/changeset APIs...
  if(wasm.bigIntEnabled
     && !!wasm.exports.sqlite3changegroup_add
     && !!wasm.exports.sqlite3session_create
     && !!wasm.exports.sqlite3_preupdate_hook /* required by the session API */){
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

    bindingSignatures.int64.push(
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
      ['sqlite3changeset_apply_v3', 'int', [
        'sqlite3*', 'int', 'void*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xFilter', signature: 'i(pp)', bindScope: 'transient'
        }),
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xConflict', signature: 'i(pip)', bindScope: 'transient'
        }),
        'void*', '**', 'int*', 'int'

      ]],
      ['sqlite3changeset_apply_v3_strm', 'int', [
        'sqlite3*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xInput', signature: 'i(ppp)', bindScope: 'transient'
        }),
        'void*',
        new wasm.xWrap.FuncPtrAdapter({
          name: 'xFilter', signature: 'i(pp)', bindScope: 'transient'
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
    );
  }/*session/changeset APIs*/

  /**
     Prepare JS<->C struct bindings for the non-opaque struct types we
     need...
  */
  sqlite3.StructBinder = globalThis.Jaccwabyt({
    heap: wasm.heap8u,
    alloc: wasm.alloc,
    dealloc: wasm.dealloc,
    bigIntEnabled: wasm.bigIntEnabled,
    pointerIR: wasm.ptr.ir,
    memberPrefix: /* Never change this: this prefix is baked into any
                     amount of code and client-facing docs. (Much
                     later: it probably should have been '$$', but see
                     the previous sentence.) */ '$'
  });
  delete globalThis.Jaccwabyt;

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
         string, map its pointer to a copy of (''+v) for the remainder
         of the application's life, and returns that pointer value for
         this call and all future calls which are passed a
         string-equivalent argument.

       Use case: sqlite3_bind_pointer(), sqlite3_result_pointer(), and
       sqlite3_value_pointer() call for "a static string and
       preferably a string literal". This converter is used to ensure
       that the string value seen by those functions is long-lived and
       behaves as they need it to, at the cost of a one-time leak of
       each distinct key.
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
       improve readability/correctness of bindingSignatures and (B)
       provide automatic conversion from higher-level representations,
       e.g. capi.sqlite3_vfs to `sqlite3_vfs*` via (capi.sqlite3_vfs
       instance).pointer.
    */
    const __xArgPtr = wasm.xWrap.argAdapter('*');
    const nilType = function(){
      /*a class which no value can ever be an instance of*/
    };
    wasm.xWrap.argAdapter('sqlite3_filename', __xArgPtr)
    ('sqlite3_context*', __xArgPtr)
    ('sqlite3_value*', __xArgPtr)
    ('void*', __xArgPtr)
    ('sqlite3_changegroup*', __xArgPtr)
    ('sqlite3_changeset_iter*', __xArgPtr)
    ('sqlite3_session*', __xArgPtr)
    ('sqlite3_stmt*', (v)=>
      __xArgPtr((v instanceof (sqlite3?.oo1?.Stmt || nilType))
           ? v.pointer : v))
    ('sqlite3*', (v)=>
      __xArgPtr((v instanceof (sqlite3?.oo1?.DB || nilType))
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
    if( wasm.exports.sqlite3_declare_vtab ){
      wasm.xWrap.argAdapter('sqlite3_index_info*', (v)=>
        __xArgPtr((v instanceof (capi.sqlite3_index_info || nilType))
                  ? v.pointer : v))
      ('sqlite3_module*', (v)=>
        __xArgPtr((v instanceof (capi.sqlite3_module || nilType))
                  ? v.pointer : v)
      );
    }

    /**
       Alias `T*` to `*` for return type conversions for common T
       types, primarily to improve legibility of their binding
       signatures.
    */
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
    for(const e of bindingSignatures.core){
      capi[e[0]] = wasm.xWrap.apply(null, e);
    }
    for(const e of bindingSignatures.wasmInternal){
      util[e[0]] = wasm.xWrap.apply(null, e);
    }

    /* For C API functions which cannot work properly unless
       wasm.bigIntEnabled is true, install a bogus impl which throws
       if called when bigIntEnabled is false. The alternative would be
       to elide these functions altogether, which seems likely to
       cause more confusion, as documented APIs would resolve to the
       undefined value in incompatible builds. */
    for(const e of bindingSignatures.int64){
      capi[e[0]] = wasm.bigIntEnabled
        ? wasm.xWrap.apply(null, e)
        : ()=>toss(e[0]+"() is unavailable due to lack",
                   "of BigInt support in this build.");
    }

    /* We're done with bindingSignatures but it's const, so... */
    delete bindingSignatures.core;
    delete bindingSignatures.int64;
    delete bindingSignatures.wasmInternal;

    /**
       Sets the given db's error state. Accepts:

       - (sqlite3*, int code, string msg)
       - (sqlite3*, Error e [,string msg = ''+e])

       If passed a WasmAllocError, the message is ignored and the
       result code is SQLITE_NOMEM. If passed any other Error type,
       the result code defaults to SQLITE_ERROR unless the Error
       object has a resultCode property, in which case that is used
       (e.g. SQLite3Error has that). If passed a non-WasmAllocError
       exception, the message string defaults to ''+theError.

       Returns either the final result code, capi.SQLITE_NOMEM if
       setting the message string triggers an OOM, or
       capi.SQLITE_MISUSE if pDb is NULL or invalid (with the caveat
       that behavior in the later case is undefined if pDb is not
       "valid enough").

       Pass (pDb,0,0) to clear the error state.
    */
    util.sqlite3__wasm_db_error = function(pDb, resultCode, message){
      if( !pDb ) return capi.SQLITE_MISUSE;
      if(resultCode instanceof sqlite3.WasmAllocError){
        resultCode = capi.SQLITE_NOMEM;
        message = 0 /*avoid allocating message string*/;
      }else if(resultCode instanceof Error){
        message = message || ''+resultCode;
        resultCode = (resultCode.resultCode || capi.SQLITE_ERROR);
      }
      return capi.sqlite3_set_errmsg(pDb, resultCode, message) || resultCode;
    };
  }/*xWrap() bindings*/

  {/* Import C-level constants and structs... */
    const cJson = wasm.xCall('sqlite3__wasm_enum_json');
    if(!cJson){
      toss("Maintenance required: increase sqlite3__wasm_enum_json()'s",
           "static buffer size!");
    }
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
                          'version'];
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
      ** them to WASM requires that we create top-level structs to
      ** model them with, but those are no longer needed after we've
      ** passed them to StructBinder. */
      for(const k of ['sqlite3_index_constraint',
                      'sqlite3_index_orderby',
                      'sqlite3_index_constraint_usage']){
        capi.sqlite3_index_info[k] = capi[k];
        delete capi[k];
      }
      capi.sqlite3_vtab_config = wasm.xWrap(
        'sqlite3__wasm_vtab_config','int',[
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
    return util.sqlite3__wasm_db_error(pDb, capi.SQLITE_MISUSE,
                                      f+"() requires "+n+" argument"+
                                      (1===n?"":'s')+".");
  };

  /** Code duplication reducer for functions which take an encoding
      argument and require SQLITE_UTF8.  Sets the db error code to
      SQLITE_FORMAT, installs a descriptive error message,
      and returns SQLITE_FORMAT. */
  const __errEncoding = (pDb)=>{
    return util.sqlite3__wasm_db_error(
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

  if( wasm.exports.sqlite3_create_window_function ){
    __dbCleanupMap.addWindowFunc = function(pDb, name, arity){
      const m = __dbCleanupMap(pDb, 1);
      if(!m.wudf) m.wudf = new Map;
      this._addUDF(pDb, name, arity, m.wudf);
    };
  }

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
    for(const obj of [
      /* pairs of [funcName, itsArityInC] */
      ['sqlite3_busy_handler',3],
      ['sqlite3_commit_hook',3],
      ['sqlite3_preupdate_hook',3],
      ['sqlite3_progress_handler',4],
      ['sqlite3_rollback_hook',3],
      ['sqlite3_set_authorizer',3],
      ['sqlite3_trace_v2', 4],
      ['sqlite3_update_hook',3]
      /*
        We do not yet have a way to clean up automatically-converted
        sqlite3_set_auxdata() finalizers.
      */
    ]){
      const [name, arity] = obj;
      const x = wasm.exports[name];
      if( !x ){
        /* assume it was built without this API */
        continue;
      }
      const closeArgs = [pDb];
      closeArgs.length = arity
      /* Recall that: (A) undefined entries translate to 0 when
         passed to WASM and (B) Safari wraps wasm.exports.* in
         nullary functions so x.length is 0 there. */;
      //wasm.xWrap.debug = true;
      try{ capi[name](...closeArgs) }
      catch(e){
        /* This "cannot happen" unless something is well and truly sideways. */
        sqlite3.config.warn("close-time call of",name+"(",closeArgs,") threw:",e);
      }
      //wasm.xWrap.debug = false;
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

  if(capi.sqlite3session_create){
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
          are "permanent," in that they will stay in the WASM
          environment until it shuts down unless the client calls this
          again with the same collation name and a value of 0 or null
          for the the function pointer(s). sqlite3_close_v2() will
          also clean up such automatically-installed WASM functions.

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
        return util.sqlite3__wasm_db_error(pDb, e);
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

    const __sqlite3CreateWindowFunction =
          wasm.exports.sqlite3_create_window_function
          ? wasm.xWrap(
            "sqlite3_create_window_function", "int", [
              "sqlite3*", "string"/*funcName*/, "int"/*nArg*/,
              "int"/*eTextRep*/, "*"/*pApp*/,
              new wasm.xWrap.FuncPtrAdapter({name: 'xStep', ...__cfProxy.xInverseAndStep}),
              new wasm.xWrap.FuncPtrAdapter({name: 'xFinal', ...__cfProxy.xFinalAndValue}),
              new wasm.xWrap.FuncPtrAdapter({name: 'xValue', ...__cfProxy.xFinalAndValue}),
              new wasm.xWrap.FuncPtrAdapter({name: 'xInverse', ...__cfProxy.xInverseAndStep}),
              new wasm.xWrap.FuncPtrAdapter({name: 'xDestroy', ...__cfProxy.xDestroy})
            ]
          )
          : undefined;

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
        return util.sqlite3__wasm_db_error(pDb, e, "Creation of UDF threw: "+e);
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
    if( __sqlite3CreateWindowFunction ){
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
          return util.sqlite3__wasm_db_error(pDb, e, "Creation of UDF threw: "+e);
        }
      };
    }else{
      delete capi.sqlite3_create_window_function;
    }
    /**
       A _deprecated_ alias for capi.sqlite3_result_js() which
       predates the addition of that function in the public API.
    */
    capi.sqlite3_create_function_v2.udfSetResult =
      capi.sqlite3_create_function.udfSetResult = capi.sqlite3_result_js;
    if(capi.sqlite3_create_window_function){
      capi.sqlite3_create_window_function.udfSetResult = capi.sqlite3_result_js;
    }

    /**
       A _deprecated_ alias for capi.sqlite3_values_to_js() which
       predates the addition of that function in the public API.
    */
    capi.sqlite3_create_function_v2.udfConvertArgs =
      capi.sqlite3_create_function.udfConvertArgs = capi.sqlite3_values_to_js;
    if(capi.sqlite3_create_window_function){
      capi.sqlite3_create_window_function.udfConvertArgs = capi.sqlite3_values_to_js;
    }

    /**
       A _deprecated_ alias for capi.sqlite3_result_error_js() which
       predates the addition of that function in the public API.
    */
    capi.sqlite3_create_function_v2.udfSetError =
      capi.sqlite3_create_function.udfSetError = capi.sqlite3_result_error_js;
    if(capi.sqlite3_create_window_function){
      capi.sqlite3_create_window_function.udfSetError = capi.sqlite3_result_error_js;
    }

  }/*sqlite3_create_function_v2() and sqlite3_create_window_function() proxies*/;

  {/* Special-case handling of sqlite3_prepare_v2() and
      sqlite3_prepare_v3() */

    /**
       Helper for string:flexible conversions which requires a
       byte-length counterpart argument. Passed a value and its
       ostensible length, this function returns [V,N], where V is
       either v or a to-string transformed copy of v and N is either n
       (if v is a WASM pointer, in which case n might be a BigInt), -1
       (if v is a string or Array), or the byte length of v (if it's a
       byte array or ArrayBuffer).
    */
    const __flexiString = (v,n)=>{
      if('string'===typeof v){
        n = -1;
      }else if(util.isSQLableTypedArray(v)){
        n = v.byteLength;
        v = wasm.typedArrayToString(
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
         Impl which requires that the 2nd argument be a pointer to the
         SQL string, instead of being converted to a JS string. This
         variant is necessary for cases where we require a non-NULL
         value for the final argument (prepare/step of multiple
         statements from one input string). For simpler cases, where
         only the first statement in the SQL string is required, the
         wrapper named sqlite3_prepare_v2() is sufficient and easier
         to use because it doesn't require dealing with pointers.
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
      const [xSql, xSqlLen] = __flexiString(sql, Number(sqlLen));
      switch(typeof xSql){
        case 'string': return __prepare.basic(pDb, xSql, xSqlLen, prepFlags, ppStmt, null);
        case (typeof wasm.ptr.null):
          return __prepare.full(pDb, wasm.ptr.coerce(xSql), xSqlLen, prepFlags,
                                ppStmt, pzTail);
        default:
          return util.sqlite3__wasm_db_error(
            pDb, capi.SQLITE_MISUSE,
            "Invalid SQL argument type for sqlite3_prepare_v2/v3(). typeof="+(typeof xSql)
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
          return util.sqlite3__wasm_db_error(
            capi.sqlite3_db_handle(pStmt), capi.SQLITE_MISUSE,
            "Invalid 3rd argument type for sqlite3_bind_text()."
          );
        }
        return __bindText(pStmt, iCol, p, n, capi.SQLITE_WASM_DEALLOC);
      }catch(e){
        wasm.dealloc(p);
        return util.sqlite3__wasm_db_error(
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
          return util.sqlite3__wasm_db_error(
            capi.sqlite3_db_handle(pStmt), capi.SQLITE_MISUSE,
            "Invalid 3rd argument type for sqlite3_bind_blob()."
          );
        }
        return __bindBlob(pStmt, iCol, p, n, capi.SQLITE_WASM_DEALLOC);
      }catch(e){
        wasm.dealloc(p);
        return util.sqlite3__wasm_db_error(
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
            return wasm.exports.sqlite3__wasm_config_i(op, args[0]);
          case capi.SQLITE_CONFIG_LOOKASIDE: // 13  /* int int */
            return wasm.exports.sqlite3__wasm_config_ii(op, args[0], args[1]);
          case capi.SQLITE_CONFIG_MEMDB_MAXSIZE: // 29  /* sqlite3_int64 */
            return wasm.exports.sqlite3__wasm_config_j(op, args[0]);
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
          /* maintenance note: we specifically do not include
             SQLITE_CONFIG_ROWID_IN_VIEW here, on the grounds that
             it's only for legacy support and no apps written with
             this API require that. */
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
        wasm.exports.sqlite3__wasm_kvvfs_methods()
      );
      delete capi.sqlite3_kvvfs_methods;

      const kvvfsMakeKey = wasm.exports.sqlite3__wasm_kvvfsMakeKeyOnPstack,
            pstack = wasm.pstack;

      const kvvfsStorage = (zClass)=>
            ((115/*=='s'*/===wasm.peek(zClass))
             ? sessionStorage : localStorage);

      /**
         Implementations for members of the object referred to by
         sqlite3__wasm_kvvfs_methods(). We swap out the native
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
            const nV = jV.length /* We are relying 100% on v being
                                    ASCII so that jV.length is equal
                                    to the C-string's byte length. */;
            if(nBuf<=0) return nV;
            else if(1===nBuf){
              wasm.poke(zBuf, 0);
              return nV;
            }
            const zV = wasm.scopedAllocCString(jV);
            if(nBuf > nV + 1) nBuf = nV + 1;
            wasm.heap8u().copyWithin(
              Number(zBuf), Number(zV), wasm.ptr.addn(zV, nBuf,- 1)
            );
            wasm.poke(wasm.ptr.add(zBuf, nBuf, -1), 0);
            return nBuf - 1;
          }catch(e){
            sqlite3.config.error("kvstorageRead()",e);
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
            sqlite3.config.error("kvstorageWrite()",e);
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
            sqlite3.config.error("kvstorageDelete()",e);
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

  /* Warn if client-level code makes use of FuncPtrAdapter. */
  wasm.xWrap.FuncPtrAdapter.warnOnUse = true;

  const StructBinder = sqlite3.StructBinder
  /* we require a local alias b/c StructBinder is removed from the sqlite3
     object during the final steps of the API cleanup. */;
  /**
     Installs a StructBinder-bound function pointer member of the
     given name and function in the given StructBinder.StructType
     target object.

     It creates a WASM proxy for the given function and arranges for
     that proxy to be cleaned up when tgt.dispose() is called. Throws
     on the slightest hint of error, e.g. tgt is-not-a StructType,
     name does not map to a struct-bound member, etc.

     As a special case, if the given function is a pointer, then
     `wasm.functionEntry()` is used to validate that it is a known
     function. If so, it is used as-is with no extra level of proxying
     or cleanup, else an exception is thrown. It is legal to pass a
     value of 0, indicating a NULL pointer, with the caveat that 0
     _is_ a legal function pointer in WASM but it will not be accepted
     as such _here_. (Justification: the function at address zero must
     be one which initially came from the WASM module, not a method we
     want to bind to a virtual table or VFS.)

     This function returns a proxy for itself which is bound to tgt
     and takes 2 args (name,func). That function returns the same
     thing as this one, permitting calls to be chained.

     If called with only 1 arg, it has no side effects but returns a
     func with the same signature as described above.

     ACHTUNG: because we cannot generically know how to transform JS
     exceptions into result codes, the installed functions do no
     automatic catching of exceptions. It is critical, to avoid
     undefined behavior in the C layer, that methods mapped via
     this function do not throw. The exception, as it were, to that
     rule is...

     If applyArgcCheck is true then each JS function (as opposed to
     function pointers) gets wrapped in a proxy which asserts that it
     is passed the expected number of arguments, throwing if the
     argument count does not match expectations. That is only intended
     for dev-time usage for sanity checking, and may leave the C
     environment in an undefined state.
  */
  const installMethod = function callee(
    tgt, name, func, applyArgcCheck = callee.installMethodArgcCheck
  ){
    if(!(tgt instanceof StructBinder.StructType)){
      toss("Usage error: target object is-not-a StructType.");
    }else if(!(func instanceof Function) && !wasm.isPtr(func)){
      toss("Usage error: expecting a Function or WASM pointer to one.");
    }
    if(1===arguments.length){
      return (n,f)=>callee(tgt, n, f, applyArgcCheck);
    }
    if(!callee.argcProxy){
      callee.argcProxy = function(tgt, funcName, func,sig){
        return function(...args){
          if(func.length!==arguments.length){
            toss("Argument mismatch for",
                 tgt.structInfo.name+"::"+funcName
                 +": Native signature is:",sig);
          }
          return func.apply(this, args);
        }
      };
      /* An ondispose() callback for use with
         StructBinder-created types. */
      callee.removeFuncList = function(){
        if(this.ondispose.__removeFuncList){
          this.ondispose.__removeFuncList.forEach(
            (v,ndx)=>{
              if(wasm.isPtr(v)){
                try{wasm.uninstallFunction(v)}
                catch(e){/*ignore*/}
              }
              /* else it's a descriptive label for the next number in
                 the list. */
            }
          );
          delete this.ondispose.__removeFuncList;
        }
      };
    }/*static init*/
    const sigN = tgt.memberSignature(name);
    if(sigN.length<2){
      toss("Member",name,"does not have a function pointer signature:",sigN);
    }
    const memKey = tgt.memberKey(name);
    const fProxy = (applyArgcCheck && !wasm.isPtr(func))
    /** This middle-man proxy is only for use during development, to
        confirm that we always pass the proper number of
        arguments. We know that the C-level code will always use the
        correct argument count. */
          ? callee.argcProxy(tgt, memKey, func, sigN)
          : func;
    if(wasm.isPtr(fProxy)){
      if(fProxy && !wasm.functionEntry(fProxy)){
        toss("Pointer",fProxy,"is not a WASM function table entry.");
      }
      tgt[memKey] = fProxy;
    }else{
      const pFunc = wasm.installFunction(fProxy, tgt.memberSignature(name));
      tgt[memKey] = pFunc;
      if(!tgt.ondispose || !tgt.ondispose.__removeFuncList){
        tgt.addOnDispose('ondispose.__removeFuncList handler',
                         callee.removeFuncList);
        tgt.ondispose.__removeFuncList = [];
      }
      tgt.ondispose.__removeFuncList.push(memKey, pFunc);
    }
    return (n,f)=>callee(tgt, n, f, applyArgcCheck);
  }/*installMethod*/;
  installMethod.installMethodArgcCheck = false;

  /**
     Installs methods into the given StructBinder.StructType-type
     instance. Each entry in the given methods object must map to a
     known member of the given StructType, else an exception will be
     triggered.  See installMethod() for more details, including the
     semantics of the 3rd argument.

     As an exception to the above, if any two or more methods in the
     2nd argument are the exact same function, installMethod() is
     _not_ called for the 2nd and subsequent instances, and instead
     those instances get assigned the same method pointer which is
     created for the first instance. This optimization is primarily to
     accommodate special handling of sqlite3_module::xConnect and
     xCreate methods.

     On success, returns its first argument. Throws on error.
  */
  const installMethods = function(
    structInstance, methods, applyArgcCheck = installMethod.installMethodArgcCheck
  ){
    const seen = new Map /* map of <Function, memberName> */;
    for(const k of Object.keys(methods)){
      const m = methods[k];
      const prior = seen.get(m);
      if(prior){
        const mkey = structInstance.memberKey(k);
        structInstance[mkey] = structInstance[structInstance.memberKey(prior)];
      }else{
        installMethod(structInstance, k, m, applyArgcCheck);
        seen.set(m, k);
      }
    }
    return structInstance;
  };

  /**
     Equivalent to calling installMethod(this,...arguments) with a
     first argument of this object. If called with 1 or 2 arguments
     and the first is an object, it's instead equivalent to calling
     installMethods(this,...arguments).
  */
  StructBinder.StructType.prototype.installMethod = function callee(
    name, func, applyArgcCheck = installMethod.installMethodArgcCheck
  ){
    return (arguments.length < 3 && name && 'object'===typeof name)
      ? installMethods(this, ...arguments)
      : installMethod(this, ...arguments);
  };

  /**
     Equivalent to calling installMethods() with a first argument
     of this object.
  */
  StructBinder.StructType.prototype.installMethods = function(
    methods, applyArgcCheck = installMethod.installMethodArgcCheck
  ){
    return installMethods(this, methods, applyArgcCheck);
  };

});
