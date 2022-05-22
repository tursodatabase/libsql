/*
  2022-05-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A basic test script for sqlite3-api.js.
*/
const setupAPI = function(S/*sqlite3 module*/){

    /* memory for use in some pointer-passing routines */
    const pPtrArg = stackAlloc(4);
    const dummyArg = {/*for restricting Stmt constructor to internal use*/};
    const toss = function(){
        throw new Error(Array.prototype.join.apply(arguments, ' '));
    };

    /**
       The DB class wraps a sqlite3 db handle.
    */
    const DB = function(name/*TODO: openMode flags*/){
        if(!name) name = ':memory:';
        else if('string'!==typeof name){
            toss("TODO: support blob image of db here.");
        }
        this.checkRc(S.sqlite3_open(name, pPtrArg));
        this.pDb = getValue(pPtrArg, "i32");
        this.filename = name;
        this._statements = {/*array of open Stmt _pointers_*/};
    };

    /**
       This class wraps sqlite3_stmt. Calling this constructor
       directly will trigger an exception. Use DB.prepare() to create
       new instances.
    */
    const Stmt = function(){
        if(dummyArg!=arguments[2]){
            toss("Do not call the Stmt constructor directly. Use DB.prepare().");
        }
        this.db = arguments[0];
        this.pStmt = arguments[1];
        this.columnCount = S.sqlite3_column_count(this.pStmt);
        this._allocs = [/*list of alloc'd memory blocks for bind() values*/]
    };


    /** Throws if the given DB has been closed, else it is returned. */
    const affirmDbOpen = function(db){
        if(!db.pDb) toss("DB has been closed.");
        return db;
    };
    
    DB.prototype = {
        /**
           Expects to be given an sqlite3 API result code. If it is
           falsy, this function returns this object, else it throws an
           exception with an error message from sqlite3_errmsg(),
           using this object's db handle.
        */
        checkRc: function(sqliteResultCode){
            if(!sqliteResultCode) return this;
            toss(S.sqlite3_errmsg(this.pDb) || "Unknown db error.");
        },
        /**
           Finalizes all open statements and closes this database
           connection. This is a no-op if the db has already been
           closed.
        */
        close: function(){
            if(this.pDb){
                let s;
                while(undefined!==(s = this._statements.pop())){
                    if(s.pStmt) s.finalize();
                }
                S.sqlite3_close_v2(this.pDb);
                delete this.pDb;
            }
        },
        /**
           Similar to this.filename but will return NULL for
           special names like ":memory:". Not of much use until
           we have filesystem support. Throws if the DB has
           been closed. If passed an argument it then it will return
           the filename of the ATTACHEd db with that name, else it assumes
           a name of `main`.
        */
        fileName: function(dbName){
            return S.sqlite3_db_filename(affirmDbOpen(this).pDb, dbName||"main");
        },

        /**
           Compiles the given SQL and returns a prepared Stmt. This is
           the only way to create new Stmt objects. Throws on error.
        */
        prepare: function(sql){
            affirmDbOpen(this);
            setValue(pPtrArg,0,"i32");
            this.checkRc(S.sqlite3_prepare_v2(this.pDb, sql, -1, pPtrArg, null));
            const pStmt = getValue(pPtrArg, "i32");
            if(!pStmt) toss("Empty SQL is not permitted.");
            const stmt = new Stmt(this, pStmt, dummyArg);
            this._statements[pStmt] = stmt;
            return stmt;
        }
    };

    /**
       Internal-use enum for mapping JS types to DB-bindable types.
       These do not (and need not) line up with the SQLITE_type
       values. All values in this enum must be truthy and distinct
       but they need not be numbers.
    */
    const BindTypes = {
        null: 1,
        number: 2,
        string: 3,
        boolean: 4,
        blob: 5
    };
    BindTypes['undefined'] == BindTypes.null;

    /** Returns an opaque truthy value from the BindTypes
        enum if v's type is a valid bindable type, else
        returns a falsy value. */
    const isSupportedBindType = function(v){
        let t = BindTypes[null===v ? 'null' : typeof v];
        if(t) return t;
        // TODO: handle buffer/blob types.
        return undefined;
    }

    /**
       If isSupportedBindType(v) returns a truthy value, this
       function returns that value, else it throws.
    */
    const affirmSupportedBindType = function(v){
        const t = isSupportedBindType(v);
        if(t) return t;
        toss("Unsupport bind() argument type.");
    };

    /**
       If key is a number and within range of stmt's bound parameter
       count, key is returned.

       If key is not a number then it is checked against named
       parameters. If a match is found, its index is returned.

       Else it throws.
    */
    const indexOfParam = function(stmt,key){
        const n = ('number'===typeof key)
              ? key : S.sqlite3_bind_parameter_index(stmt.pStmt, key);
        if(0===n || (n===key && (n!==(n|0)/*floating point*/))){
            toss("Invalid bind() parameter name: "+key);
        }
        else if(n>=stmt.columnCount) toss("Bind index",key,"is out of range.");
        return n;
    };

    /**
       Binds a single bound parameter value on the given stmt at the
       given index (numeric or named) using the given bindType (see
       the BindTypes enum) and value. Throws on error. Returns stmt on
       success.
    */
    const bindOne = function(stmt,ndx,bindType,val){
        affirmSupportedBindType(val);
        ndx = indexOfParam(stmt,ndx);
        let rc = 0;
        switch(bindType){
            case BindType.null:
                rc = S.sqlite3_bind_null(stmt.pStmt, ndx);
                break;
            case BindType.string:{
                const bytes = intArrayFromString(string,false);
                const pStr = allocate(bytes, ALLOC_NORMAL);
                stmt._allocs.push(pStr);
                rc = S.sqlite3_bind_text(stmt.pStmt, ndx, pStr,
                                         bytes.length, 0);
                break;
            }
            case BindType.number: {
                const m = ((val === (val|0))
                           ? (val>0xefffffff
                              ? S.sqlite3_bind_int64
                              : S.sqlite3_bind_int)
                           : S.sqlite3_bind_double);
                rc = m(stmt.pStmt, ndx, val);
                break;
            }
            case BindType.boolean:
                rc = S.sqlite3_bind_int(stmt.pStmt, ndx, val ? 1 : 0);
                break;
            case BindType.blob:
            default: toss("Unsupported bind() argument type.");
        }
        if(rc) stmt.db.checkRc(rc);
        return stmt;
    };

    /** Throws if the given Stmt has been finalized, else
        it is returned. */
    const affirmStmtOpen = function(stmt){
        if(!stmt.pStmt) toss("Stmt has been closed.");
        return stmt;
    };

    /** Frees any memory explicitly allocated for the given
        Stmt object. Returns stmt. */
    const freeBindMemory = function(stmt){
        let m;
        while(undefined !== (m = stmt._allocs.pop())){
            _free(m);
        }
        return stmt;
    };
    
    Stmt.prototype = {
        /**
           "Finalizes" this statement. This is a no-op if the
           statement has already been finalizes. Returns
           undefined. Most methods in this class will throw if called
           after this is.
        */
        finalize: function(){
            if(this.pStmt){
                freeBindMemory(this);
                S.sqlite3_finalize(this.pStmt);
                delete this.pStmt;
                delete this.db;
            }
        },
        /** Clears all bound values. Returns this object.
            Throws if this statement has been finalized. */
        clearBindings: function(){
            freeBindMemory(affirmStmtOpen(this));
            S.sqlite3_clear_bindings(this.pStmt);
            return this;
        },
        /**
           Resets this statement so that it may be step()ed again
           from the beginning. Returns this object. Throws if this
           statement has been finalized.

           If passed a truthy argument then this.clearBindings() is
           also called, otherwise any existing bindings, along with
           any memory allocated for them, are retained.
        */
        reset: function(alsoClearBinds){
            if(alsoClearBinds) this.clearBindings();
            S.sqlite3_reset(affirmStmtOpen(this).pStmt);
            return this;
        },
        /**
           Binds one or more values to its bindable parameters. It
           accepts 1 or 2 arguments:

           If passed a single argument, it must be either an array, an
           object, or a value of a bindable type (see below).

           If passed 2 arguments, the first one is the 1-based bind
           index or bindable parameter name and the second one must be
           a value of a bindable type.

           Bindable value types:

           - null or undefined is bound as NULL.

           - Numbers are bound as either doubles or integers: int64 if
             they are larger than 0xEFFFFFFF, else int32. Booleans are
             bound as integer 0 or 1. Note that doubles with no
             fractional part are bound as integers. It is not expected
             that that distinction is significant for the majority of
             clients due to sqlite3's data typing model. This API does
             not currently support the BigInt type.

           - Strings are bound as strings (use bindAsBlob() to force
             blob binding).

           - buffers (blobs) are currently TODO but will be bound as
             blobs.

           If passed an array, each element of the array is bound at
           the parameter index equal to the array index plus 1
           (because arrays are 0-based but binding is 1-based).

           If passed an object, each object key is treated as a
           bindable parameter name. The object keys _must_ match any
           bindable parameter names, including any `$`, `@`, or `:`
           prefix. Because `$` is a legal identifier chararacter in
           JavaScript, that is the suggested prefix for bindable
           parameters.

           It returns this object on success and throws on
           error. Errors include:

           - Any bind index is out of range or a named bind parameter
             does not match.

           - Any value to bind is of an unsupported type.

           - Passed no arguments or more than two.

           - The statement has been finalized.
        */
        bind: function(/*[ndx,] value*/){
            let ndx, arg;
            switch(arguments.length){
                case 1: ndx = 1; arg = arguments[0]; break;
                case 2: ndx = arguments[0]; arg = arguments[1]; break;
                default: toss("Invalid bind() arguments.");
            }
            affirmStmtOpen(this);
            if(null===arg || undefined===arg){
                /* bind NULL */
                return bindOne(this, ndx, BindType.null, arg);
            }
            else if(Array.isArray(arg)){
                /* bind each entry by index */
                if(1!==arguments.length){
                    toss("When binding an array, an index argument is not permitted.");
                }
                arg.forEach((v,i)=>bindOne(this, i+1, affirmSupportedBindType(v), v));
                return this;
            }
            else if('object'===typeof arg/*null was checked above*/){
                /* bind by name */
                if(1!==arguments.length){
                    toss("When binding an object, an index argument is not permitted.");
                }
                Object.keys(arg)
                    .forEach(k=>bindOne(this, k,
                                        affirmSupportedBindType(arg[k]),
                                        arg[k]));
                return this;
            }else{
                return bindOne(this, ndx,
                               affirmSupportedBindType(arg), arg);
            }
            toss("Should not reach this point.");
        },
        /**
           Special case of bind() which binds the given value
           using the BLOB binding mechanism instead of the default
           selected one for the value. The ndx may be a numbered
           or named bind index. The value must be of type string,
           buffer, or null/undefined (both treated as null).

            If passed a single argument, a bind index of 1 is assumed.
        */
        bindAsBlob: function(ndx,arg){
            affirmStmtOpen(this);
            if(1===arguments.length){
                ndx = 1;
                arg = arguments[0];
            }
            const t = affirmSupportedBindType(arg);
            if(BindTypes.string !== t && BindTypes.blob !== t
               && BindTypes.null !== t){
                toss("Invalid value type for bindAsBlob()");
            }
            return bindOne(this, ndx, BindType.blob, arg);
        }
    };    

    const SQLite3 = {
        version: {
            lib: S.sqlite3_libversion(),
            ooApi: "0.0.1"
        },
        DB
    };
    return SQLite3;
};

const mainTest1 = function(S/*sqlite3 module*/){
    console.log("Loaded module:",S.sqlite3_libversion(),
                S.sqlite3_sourceid());
    const oo = setupAPI(S);

    const db = new oo.DB();
    console.log("DB:",db.filename);
};

self/*window or worker*/.Module.onRuntimeInitialized = function(){
    console.log("Loading sqlite3-api.js...");
    self.Module.loadSqliteAPI(mainTest1);
};
