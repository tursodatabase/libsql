/**
   Helpers for writing sqlite3-specific tests.
*/
self/*window or worker*/.SqliteTestUtil = {
    /** Running total of the number of tests run via
        this API. */
    counter: 0,
    /**
       If expr is a function, it is called and its result
       is returned, coerced to a bool, else expr, coerced to
       a bool, is returned.
    */
    toBool: function(expr){
        return (expr instanceof Function) ? !!expr() : !!expr;
    },
    /** abort() if expr is false. If expr is a function, it
        is called and its result is evaluated.
    */
    assert: function(expr, msg){
        ++this.counter;
        if(!this.toBool(expr)) abort(msg || "Assertion failed.");
        return this;
    },
    /** Identical to assert() but throws instead of calling
        abort(). */
    affirm: function(expr, msg){
        ++this.counter;
        if(!this.toBool(expr)) throw new Error(msg || "Affirmation failed.");
        return this;
    },
    /** Calls f() and squelches any exception it throws. If it
        does not throw, this function throws. */
    mustThrow: function(f, msg){
        ++this.counter;
        let err;
        try{ f(); } catch(e){err=e;}
        if(!err) throw new Error(msg || "Expected exception.");
        return this;
    },
    /** Throws if expr is truthy or expr is a function and expr()
        returns truthy. */
    throwIf: function(expr, msg){
        ++this.counter;
        if(this.toBool(expr)) throw new Error(msg || "throwIf() failed");
        return this;
    },
    /** Throws if expr is falsy or expr is a function and expr()
        returns falsy. */
    throwUnless: function(expr, msg){
        ++this.counter;
        if(!this.toBool(expr)) throw new Error(msg || "throwUnless() failed");
        return this;
    }
};
