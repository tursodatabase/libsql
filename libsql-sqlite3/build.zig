const std = @import("std");
const crab = @import("build.crab");
const fs = std.fs;
const Build = std.Build;
const LazyPath = Build.LazyPath;

const sources = .{
    .sqlite3 = &.{
        "src/alter.c",      "src/analyze.c",       "src/attach.c",        "src/auth.c",
        "src/backup.c",     "src/bitvec.c",        "src/btmutex.c",       "src/btree.c",
        "src/build.c",      "src/callback.c",      "src/complete.c",      "src/ctime.c",
        "src/date.c",       "src/dbpage.c",        "src/dbstat.c",        "src/delete.c",
        "src/expr.c",       "src/fault.c",         "src/fkey.c",          "src/func.c",
        "src/global.c",     "src/hash.c",          "src/insert.c",        "src/json.c",
        "src/legacy.c",     "src/loadext.c",       "src/main.c",          "src/malloc.c",
        "src/mem0.c",       "src/mem1.c",          "src/mem2.c",          "src/mem3.c",
        "src/mem5.c",       "src/memdb.c",         "src/memjournal.c",    "src/mutex.c",
        "src/mutex_noop.c", "src/mutex_unix.c",    "src/mutex_w32.c",     "src/notify.c",
        "src/os.c",         "src/os_kv.c",         "src/os_unix.c",       "src/os_win.c",
        "src/pager.c",      "src/pcache.c",        "src/pcache1.c",       "src/pragma.c",
        "src/prepare.c",    "src/printf.c",        "src/random.c",        "src/resolve.c",
        "src/rowset.c",     "src/select.c",        "src/status.c",        "src/table.c",
        "src/threads.c",    "src/tokenize.c",      "src/treeview.c",      "src/trigger.c",
        "src/utf.c",        "src/update.c",        "src/upsert.c",        "src/util.c",
        "src/vacuum.c",     "src/vdbe.c",          "src/vdbeapi.c",       "src/vdbeaux.c",
        "src/vdbeblob.c",   "src/vdbemem.c",       "src/vdbesort.c",      "src/vdbetrace.c",
        "src/vdbevtab.c",   "src/vtab.c",          "src/wal.c",           "src/walker.c",
        "src/where.c",      "src/wherecode.c",     "src/whereexpr.c",     "src/window.c",
        "src/vector.c",     "src/vectorfloat32.c", "src/vectorfloat64.c",
    },
    .fuzzcheck = &.{
        "test/fuzzcheck.c",
        "test/ossfuzz.c",
        "test/fuzzinvariants.c",
        "ext/recover/dbdata.c",
        "ext/recover/sqlite3recover.c",
        "test/vt02.c",
    },
    .testfixture = &.{
        "src/test1.c",                "src/test2.c",
        "src/test3.c",                "src/test4.c",
        "src/test5.c",                "src/test6.c",
        "src/test8.c",                "src/test9.c",
        "src/test_autoext.c",         "src/test_async.c",
        "src/test_backup.c",          "src/test_bestindex.c",
        "src/test_blob.c",            "src/test_btree.c",
        "src/test_config.c",          "src/test_delete.c",
        "src/test_demovfs.c",         "src/test_devsym.c",
        "src/test_fs.c",              "src/test_func.c",
        "src/test_hexio.c",           "src/test_init.c",
        "src/test_intarray.c",        "src/test_journal.c",
        "src/test_malloc.c",          "src/test_md5.c",
        "src/test_multiplex.c",       "src/test_mutex.c",
        "src/test_onefile.c",         "src/test_osinst.c",
        "src/test_pcache.c",          "src/test_quota.c",
        "src/test_rtree.c",           "src/test_schema.c",
        "src/test_superlock.c",       "src/test_syscall.c",
        "src/test_tclsh.c",           "src/test_tclvar.c",
        "src/test_thread.c",          "src/test_vdbecov.c",
        "src/test_vfs.c",             "src/test_windirent.c",
        "src/test_window.c",          "src/test_wsd.c",
        "src/tclsqlite.c",            "ext/rbu/test_rbu.c",
        "ext/misc/cksumvfs.c",        "ext/expert/sqlite3expert.c",
        "ext/expert/test_expert.c",   "ext/misc/amatch.c",
        "ext/misc/appendvfs.c",       "ext/misc/basexx.c",
        "ext/misc/carray.c",          "ext/misc/closure.c",
        "ext/misc/csv.c",             "ext/misc/decimal.c",
        "ext/misc/eval.c",            "ext/misc/explain.c",
        "ext/misc/fileio.c",          "ext/misc/fuzzer.c",
        "ext/fts5/fts5_tcl.c",        "ext/fts5/fts5_test_mi.c",
        "ext/fts5/fts5_test_tok.c",   "ext/misc/ieee754.c",
        "ext/misc/mmapwarm.c",        "ext/misc/nextchar.c",
        "ext/misc/normalize.c",       "ext/misc/percentile.c",
        "ext/misc/prefixes.c",        "ext/misc/qpvtab.c",
        "ext/misc/regexp.c",          "ext/misc/remember.c",
        "ext/misc/series.c",          "ext/misc/spellfix.c",
        "ext/misc/totype.c",          "ext/misc/unionvtab.c",
        "ext/misc/wholenumber.c",     "ext/misc/zipfile.c",
        "ext/userauth/userauth.c",    "ext/rtree/test_rtreedoc.c",
        "ext/recover/test_recover.c", "ext/recover/sqlite3recover.c",
        "ext/recover/dbdata.c",       "ext/session/test_session.c",
        "ext/fts3/fts3_test.c",
    },
    .fts5 = &.{
        "ext/fts5/fts5_aux.c",      "ext/fts5/fts5_buffer.c",
        "ext/fts5/fts5_config.c",   "ext/fts5/fts5_expr.c",
        "ext/fts5/fts5_hash.c",     "ext/fts5/fts5_index.c",
        "ext/fts5/fts5_main.c",     "ext/fts5/fts5_storage.c",
        "ext/fts5/fts5_tokenize.c", "ext/fts5/fts5_unicode2.c",
        "ext/fts5/fts5_varint.c",   "ext/fts5/fts5_vocab.c",
    },
};

pub const Amalgamation = struct {
    step: Build.Step,
    lazy_paths: std.ArrayList(std.Build.LazyPath),
    output_list: Build.GeneratedFile,
    basename: []const u8,

    pub fn create(b: *Build, basename: []const u8, lazy_paths: []const LazyPath) *Amalgamation {
        const self = b.allocator.create(Amalgamation) catch @panic("OOM");

        self.* = .{
            .step = std.Build.Step.init(.{
                .id = .custom,
                .name = "amalgamate files",
                .owner = b,
                .makeFn = make,
            }),
            .lazy_paths = undefined,
            .basename = basename,
            .output_list = .{ .step = &self.step },
        };

        var list = std.ArrayList(LazyPath).init(b.allocator);

        for (lazy_paths) |lp| {
            list.append(lp) catch @panic("OOM");
            lp.addStepDependencies(&self.step);
        }

        self.lazy_paths = list;

        return self;
    }

    pub fn getOutput(self: *const Amalgamation) LazyPath {
        return .{ .generated = .{ .file = &self.output_list } };
    }

    pub fn make(step: *Build.Step, prog_node: std.Progress.Node) !void {
        _ = prog_node;
        const self: *Amalgamation = @fieldParentPtr("step", step);
        const b = step.owner;

        var man = b.graph.cache.obtain();
        defer man.deinit();

        var output = std.ArrayList(u8).init(b.allocator);
        defer output.deinit();

        for (self.lazy_paths.items) |lp| {
            const file = try std.fs.cwd().readFileAlloc(
                b.allocator,
                lp.getPath2(b, step),
                2 * 1024 * 1024,
            );
            defer b.allocator.free(file);

            try std.fmt.format(output.writer(),
                \\/* amalg:begin {s} */
                \\
            , .{lp.getPath(b)});

            try output.appendSlice(file);

            try std.fmt.format(output.writer(),
                \\/* amalg:end {s} */
                \\
            , .{lp.getPath(b)});
        }

        man.hash.addBytes(output.items);

        if (try step.cacheHit(&man)) {
            const digest = man.final();
            self.output_list.path = try b.cache_root.join(b.allocator, &.{ "o", &digest, self.basename });
            return;
        }

        const digest = man.final();

        const sub_path = b.pathJoin(&.{ "o", &digest, self.basename });
        const sub_path_dirname = std.fs.path.dirname(sub_path).?;

        try b.cache_root.handle.makePath(sub_path_dirname);
        try b.cache_root.handle.writeFile(.{
            .sub_path = sub_path,
            .data = output.items,
        });
        self.output_list.path = try b.cache_root.join(b.allocator, &.{sub_path});

        try man.writeManifest();
    }
};

fn cflags(b: *Build, flags: []const []const u8) [][]const u8 {
    return std.mem.concat(b.allocator, []const u8, &.{
        &.{
            "-g",
            // "-O3",
            "-DBUILD_sqlite",
            "-DNDEBUG",
            "-DSQLITE_ENABLE_MATH_FUNCTIONS",
            // "-DSQLITE_TEMP_STORE=2",
            // "-DSQLITE_USE_URI=1",
            "-DSQLITE_THREADSAFE=1",
            "-D_HAVE_SQLITE_CONFIG",
            "-DSQLITE_CORE",
        },
        flags,
    }) catch @panic("OOM");
}

const Sqlite3Options = struct {
    target: Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    wasm_runtime: bool,
    icu: bool,
    fts3: bool,
    fts5: bool,
    geopoly: bool,
    rtree: bool,
    session: bool,
    @"test": bool,
};

fn filterIncludes(b: *Build, lp: LazyPath) LazyPath {
    const filtered = b.addSystemCommand(&.{ "grep", "-v", "#include" });
    filtered.addFileArg(lp);
    return filtered.captureStdOut();
}

fn addLibsql(b: *Build, options: Sqlite3Options) *Build.Step.Compile {
    const sqlite_cfg = b.addConfigHeader(
        .{
            .include_path = "sqlite_cfg.h",
            .style = .{ .autoconf = b.path("sqlite_cfg.h.in") },
        },
        .{
            .HAVE_FDATASYNC = 1, // Define to 1 if you have the `fdatasync' function.
            .HAVE_GMTIME_R = 1, // only use is in src/data.c:1578

            // available in C99, better remove this later and assume C99 compatibility
            .HAVE_STDINT_H = 1,

            // has the same stuff as <stdint.h>, with more bloat -- remove
            // later in sqliteInt.h
            .HAVE_INTTYPES_H = 1,

            // this should be allways available
            .HAVE_INT16_T = 1,
            .HAVE_INT32_T = 1,
            .HAVE_INT64_T = 1,
            .HAVE_INT8_T = 1,
            .HAVE_INTPTR_T = 1,
            .HAVE_UINT16_T = 1,
            .HAVE_UINT32_T = 1,
            .HAVE_UINT64_T = 1,
            .HAVE_UINT8_T = 1,
            .HAVE_UINTPTR_T = 1,

            // available in C99
            .HAVE_ISNAN = 1,

            // available in C23, previously was a non-POSIX API on Unix-like systems
            .HAVE_LOCALTIME_R = 1,

            // available in C11, previously was a Microsoft API
            .HAVE_LOCALTIME_S = 1,

            // generaly avaiable, non-POSIX
            .HAVE_MALLOC_H = 1,

            // malloc_usable_size is defined in <malloc.h> for unix systems,
            // MacOS seems to not have it, not sure
            // TODO: invetigate the use of this API
            .HAVE_MALLOC_USABLE_SIZE = 1,

            // TODO: stage removal, useful but not used
            .HAVE_PREAD = 1,
            .HAVE_PWRITE = 1,

            // Legacy API available only on GLibc and deprecated in musl, remove this later
            .HAVE_PREAD64 = null,
            .HAVE_PWRITE64 = null,
            ._FILE_OFFSET_BITS = null,

            // used in src/printf.c:227, seems to be a GNU-ism, but fine
            .HAVE_STRCHRNUL = 1,

            .HAVE_USLEEP = 1, // src/os_unix.c *important for performance*
            .HAVE_UTIME = 1, // src/os_unix.c src/vxworks.h

            .PACKAGE_VERSION = "", // Define to the version of this package.

            // never used
            .HAVE_DLFCN_H = null,
            .HAVE_MEMORY_H = null,
            .HAVE_STDLIB_H = null,
            .HAVE_STRINGS_H = null,
            .HAVE_STRING_H = null,
            .HAVE_SYS_STAT_H = null,
            .HAVE_SYS_TYPES_H = null,
            .HAVE_UNISTD_H = null,
            .HAVE_ZLIB_H = null,
            .LT_OBJDIR = null,
            .PACKAGE_BUGREPORT = null,
            .PACKAGE_NAME = null,
            .PACKAGE_STRING = null,
            .PACKAGE_TARNAME = null,
            .PACKAGE_URL = null,
            .STDC_HEADERS = null,
            ._LARGE_FILES = null,
        },
    );

    const lemon = b.addExecutable(.{
        .name = "lemon",
        .root_source_file = null,
        .target = options.target,
        .optimize = .ReleaseFast,
    });
    lemon.addCSourceFile(.{ .file = b.path("tool/lemon.c") });
    lemon.linkLibC();

    var parse = parse: {
        const run = b.addRunArtifact(lemon);
        run.setCwd(b.path("tool/"));
        run.addArg("-DSQLITE_ENABLE_MATH_FUNCTIONS");
        const dir = run.addPrefixedOutputDirectoryArg("-d", ".");
        run.addArg("-S");
        run.addFileArg(b.path("src/parse.y"));

        break :parse .{
            .h = dir.path(b, "parse.h"),
            .c = dir.path(b, "parse.c"),
        };
    };

    const keywordhash = keywordhash: {
        const mkkeywordhash = b.addExecutable(.{
            .name = "mkkeywordhash",
            .root_source_file = null,
            .target = options.target,
            .optimize = .ReleaseFast,
        });
        mkkeywordhash.addCSourceFile(.{ .file = b.path("tool/mkkeywordhash.c") });
        mkkeywordhash.linkLibC();

        break :keywordhash .{
            .h = b.addWriteFiles().addCopyFile(
                b.addRunArtifact(mkkeywordhash).captureStdOut(),
                "keywordhash.h",
            ),
        };
    };

    const opcodes: struct {
        h: std.Build.LazyPath,
        c: std.Build.LazyPath,
    } = opcode: {
        const h = h: {
            const h = b.addSystemCommand(&.{"tclsh"});
            h.addFileArg(b.path("tool/mkopcodeh.tcl"));
            h.setStdIn(.{
                .lazy_path = Amalgamation.create(b, "parse_vbde", &.{
                    parse.h,
                    b.path("src/vdbe.c"),
                }).getOutput(),
            });
            break :h b.addWriteFiles().addCopyFile(
                h.captureStdOut(),
                "opcodes.h",
            );
        };

        const c = c: {
            const c = b.addSystemCommand(&.{"tclsh"});
            c.addFileArg(b.path("tool/mkopcodec.tcl"));
            c.addFileArg(h);
            break :c b.addWriteFiles().addCopyFile(
                c.captureStdOut(),
                "opcodes.c",
            );
        };

        break :opcode .{
            .h = h,
            .c = c,
        };
    };

    const h = Amalgamation.create(b, "sqlite3.h", &.{
        base: { // sqlite.h.in
            const base = b.addConfigHeader(
                .{ .style = .{ .cmake = b.path("src/sqlite.h.in") } },
                .{
                    // TODO: Remove hard coded values
                    .libsql_version = "0.2.3",
                    .sqlite_version = "3.44.0",
                    .sqlite_version_number = 3044000,
                    .sqlite_source_id = "2023-11-01 11:23:50 17129ba1ff7f0daf37100ee82d507aef7827cf38de1866e2633096ae6ad8alt1",
                },
            );
            break :base base.getOutput();
        },
        filterIncludes(b, b.path("ext/rtree/sqlite3rtree.h")),
        filterIncludes(b, b.path("ext/session/sqlite3session.h")),
        filterIncludes(b, b.path("ext/fts5/fts5.h")),
        b.path("src/page_header.h"), // this must be above wal.h, since it depends on this
        filterIncludes(b, b.path("src/wal.h")),
        b.path("ext/udf/wasm_bindings.h"),
    }).getOutput();

    const lib = b.addStaticLibrary(.{
        .name = "sqlite3",
        .target = options.target,
        .optimize = options.optimize,
        .link_libc = true,
    });
    lib.installHeader(h, "sqlite3.h");

    lib.addIncludePath(h.dirname());
    lib.addIncludePath(b.path("src/"));

    lib.addIncludePath(opcodes.h.dirname());
    lib.addIncludePath(keywordhash.h.dirname());
    lib.addIncludePath(parse.h.dirname());
    lib.addConfigHeader(sqlite_cfg);

    lib.addCSourceFile(.{ .file = opcodes.c, .flags = cflags(b, &.{}) });
    lib.addCSourceFile(.{ .file = parse.c, .flags = cflags(b, &.{}) });
    lib.addCSourceFiles(.{ .files = sources.sqlite3, .flags = cflags(b, &.{}) });
    lib.addCSourceFile(.{ .file = b.path("ext/misc/stmt.c"), .flags = cflags(b, &.{}) });

    if (options.fts5) {
        var fts5parse = fts5parse: {
            const run = b.addRunArtifact(lemon);
            run.setCwd(b.path("tool/"));
            run.addArg("-DSQLITE_ENABLE_MATH_FUNCTIONS");
            const dir = run.addPrefixedOutputDirectoryArg("-d", ".");
            run.addArg("-S");
            run.addFileArg(b.path("ext/fts5/fts5parse.y"));

            break :fts5parse .{
                .h = dir.path(b, "fts5parse.h"),
                .c = dir.path(b, "fts5parse.c"),
            };
        };

        lib.addIncludePath(b.path("ext/fts5/"));
        lib.addIncludePath(fts5parse.h.dirname());

        lib.addCSourceFile(.{ .file = fts5parse.c, .flags = cflags(b, &.{}) });
        lib.addCSourceFiles(.{
            .files = &.{
                "ext/fts5/fts5_aux.c",      "ext/fts5/fts5_buffer.c",
                "ext/fts5/fts5_config.c",   "ext/fts5/fts5_expr.c",
                "ext/fts5/fts5_hash.c",     "ext/fts5/fts5_index.c",
                "ext/fts5/fts5_main.c",     "ext/fts5/fts5_storage.c",
                "ext/fts5/fts5_tokenize.c", "ext/fts5/fts5_unicode2.c",
                "ext/fts5/fts5_varint.c",   "ext/fts5/fts5_vocab.c",
            },
            .flags = cflags(b, &.{}),
        });

        lib.root_module.addCMacro("SQLITE_ENABLE_FTS5", "1");
    }

    if (options.fts3) {
        lib.addIncludePath(b.path("ext/fts3/"));
        lib.addCSourceFiles(.{
            .files = &.{
                "ext/fts3/fts3.c",
                "ext/fts3/fts3_aux.c",
                "ext/fts3/fts3_term.c",
                "ext/fts3/fts3_expr.c",
                "ext/fts3/fts3_hash.c",
                "ext/fts3/fts3_icu.c",
                "ext/fts3/fts3_porter.c",
                "ext/fts3/fts3_snippet.c",
                "ext/fts3/fts3_tokenizer.c",
                "ext/fts3/fts3_tokenizer1.c",
                "ext/fts3/fts3_tokenize_vtab.c",
                "ext/fts3/fts3_unicode.c",
                "ext/fts3/fts3_unicode2.c",
                "ext/fts3/fts3_write.c",
            },
            .flags = cflags(b, &.{}),
        });

        lib.root_module.addCMacro("SQLITE_ENABLE_FTS3", "1");
        lib.root_module.addCMacro("SQLITE_ENABLE_FTS3_PARENTHESIS", "1");
    }

    if (options.rtree) {
        lib.addCSourceFile(.{ .file = b.path("ext/rtree/rtree.c"), .flags = cflags(b, &.{}) });
        lib.root_module.addCMacro("SQLITE_ENABLE_RTREE", "1");
        if (options.geopoly) lib.root_module.addCMacro("SQLITE_ENABLE_GEOPOLY", "1");
    }

    if (options.wasm_runtime) {
        const libsql_wasm = crab.addCargoBuild(b, .{
            .manifest_path = b.path("crates/wasmtime-bindings/Cargo.toml"),
            .cargo_args = &.{
                "--release",
                "--lib",
            },
        }, .{
            .target = options.target,
            .optimize = .ReleaseSafe,
        });

        lib.root_module.addCMacro("LIBSQL_ENABLE_WASM_RUNTIME", "");
        lib.addIncludePath(b.path(".")); // to reach "ext/udf/wasm_bindings.h"
        lib.addCSourceFile(.{
            .file = b.path("ext/udf/wasmedge_bindings.c"),
            .flags = cflags(b, &.{}),
        });
        lib.addLibraryPath(libsql_wasm);
        lib.linkSystemLibrary("libsql_wasm");
    }

    if (options.icu) {
        lib.addCSourceFiles(.{
            .files = &.{"ext/icu/icu.c"},
            .flags = cflags(b, &.{}),
        });

        lib.root_module.addCMacro("SQLITE_ENABLE_ICU", "1");
        lib.linkSystemLibrary("icuuc");
        lib.linkSystemLibrary("icuio");
        lib.linkSystemLibrary("icui18n");
    }

    if (options.session) {
        lib.addIncludePath(b.path("ext/session/"));

        lib.addCSourceFiles(.{
            .files = &.{"ext/session/sqlite3session.c"},
            .flags = cflags(b, &.{}),
        });

        lib.root_module.addCMacro("SQLITE_ENABLE_SESSION", "1");
        lib.root_module.addCMacro("SQLITE_ENABLE_PREUPDATE_HOOK", "1");
    }

    if (options.@"test") {
        lib.installHeader(keywordhash.h, "keywordhash.h");
        lib.installHeader(opcodes.h, "opcodes.h");

        lib.root_module.addCMacro("SQLITE_DEFAULT_PAGE_SIZE", "1024");
        lib.root_module.addCMacro("SQLITE_TEST", "1");
        lib.root_module.addCMacro("SQLITE_NO_SYNC", "1");

        lib.linkSystemLibrary("tcl8.6");
    }

    if (options.target.result.os.tag == .wasi) {
        lib.root_module.addCMacro("SQLITE_WASI", "1");
        lib.root_module.addCMacro("SQLITE_OMIT_SHARED_MEM", "1");
        lib.root_module.addCMacro("SQLITE_OMIT_SHARED_CACHE", "1");
    }

    if (options.target.result.os.tag == .windows) {
        lib.root_module.addCMacro("SQLITE_OS_WIN", "1");
    } else {
        lib.root_module.addCMacro("SQLITE_OS_UNIX", "1");
    }

    lib.root_module.addCMacro("SQLITE_ENABLE_DBPAGE_VTAB", "1");
    lib.root_module.addCMacro("SQLITE_ENABLE_DBSTAT_VTAB", "1");
    lib.root_module.addCMacro("SQLITE_ENABLE_STMTVTAB", "1");
    lib.root_module.addCMacro("SQLITE_ENABLE_BYTECODE_VTAB", "1");
    lib.root_module.addCMacro("SQLITE_ENABLE_COLUMN_METADATA", "1");

    lib.root_module.addCMacro("SQLITE_ENABLE_LOAD_EXTENSION", "1");

    // lib.root_module.addCMacro("SQLITE_DEFAULT_FOREIGN_KEYS", "1");
    // lib.root_module.addCMacro("SQLITE_ENABLE_API_ARMOR", "1");
    // lib.root_module.addCMacro("SQLITE_ENABLE_COLUMN_METADATA", "1");
    // lib.root_module.addCMacro("SQLITE_ENABLE_DBSTAT_VTAB", "1");
    // lib.root_module.addCMacro("SQLITE_ENABLE_FTS3", "1");
    // lib.root_module.addCMacro("SQLITE_ENABLE_FTS3_PARENTHESIS", "1");
    // lib.root_module.addCMacro("SQLITE_ENABLE_FTS5", "1");
    // lib.root_module.addCMacro("SQLITE_ENABLE_JSON1", "1");
    // lib.root_module.addCMacro("SQLITE_ENABLE_MEMORY_MANAGEMENT", "1");
    // lib.root_module.addCMacro("SQLITE_ENABLE_RTREE", "1");
    // lib.root_module.addCMacro("SQLITE_ENABLE_STAT2", "1");
    // lib.root_module.addCMacro("SQLITE_ENABLE_STAT4", "1");
    // lib.root_module.addCMacro("SQLITE_SOUNDEX", "1");
    // lib.root_module.addCMacro("SQLITE_THREADSAFE", "1");
    lib.root_module.addCMacro("SQLITE_USE_URI", "1");
    // lib.root_module.addCMacro("HAVE_USLEEP", "1");

    lib.root_module.addCMacro("SQLITE_ENABLE_EXPLAIN_COMMENTS", "1");

    if (options.wasm_runtime) {
        const libsql_wasm = crab.addCargoBuild(b, .{
            .manifest_path = b.path("crates/wasmtime-bindings/Cargo.toml"),
            .cargo_args = &.{
                "--release",
                "--lib",
            },
        }, .{
            .target = options.target,
            .optimize = .ReleaseSafe,
        });

        lib.root_module.addCMacro("LIBSQL_ENABLE_WASM_RUNTIME", "");
        lib.addIncludePath(b.path(".")); // to reach "ext/udf/wasm_bindings.h"
        lib.addCSourceFile(.{
            .file = b.path("ext/udf/wasmedge_bindings.c"),
            .flags = cflags(b, &.{}),
        });
        lib.addLibraryPath(libsql_wasm);
        lib.linkSystemLibrary("libsql_wasm");
    }

    if (options.icu) {}

    return lib;
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zlib = b.dependency("zlib", .{
        .target = target,
        .optimize = optimize,
    });

    const options: Sqlite3Options = .{
        .target = target,
        .optimize = optimize,
        .wasm_runtime = b.option(
            bool,
            "wasm-runtime",
            "Enable wasm runtime (default: false)",
        ) orelse false,
        .icu = b.option(
            bool,
            "icu",
            "Enable icu extension (default: false)",
        ) orelse false,
        .fts3 = b.option(
            bool,
            "fts3",
            "Enable fts3 extension (default: true)",
        ) orelse true,
        .fts5 = b.option(
            bool,
            "fts5",
            "Enable fts5 extension (default: true)",
        ) orelse true,
        .rtree = b.option(
            bool,
            "rtree",
            "Enable rtree extension (default: true)",
        ) orelse true,
        .session = b.option(
            bool,
            "session",
            "Enable session extension (default: true)",
        ) orelse true,
        .geopoly = b.option(
            bool,
            "geopoly",
            "Enable geopoly extension (default: true)",
        ) orelse true,
        .@"test" = false,
    };

    const libsql = addLibsql(b, options);

    const fuzzcheck = b.addExecutable(.{
        .name = "fuzzcheck",
        .root_source_file = null,
        .target = target,
        .optimize = .ReleaseFast,
    });
    fuzzcheck.addIncludePath(b.path("src/"));
    fuzzcheck.addIncludePath(b.path("ext/recover"));
    fuzzcheck.addCSourceFiles(.{
        .files = sources.fuzzcheck,
        .flags = cflags(b, &.{}),
    });
    fuzzcheck.linkSystemLibrary("m");
    fuzzcheck.linkLibrary(libsql);
    fuzzcheck.linkLibrary(zlib.artifact("z"));
    fuzzcheck.linkLibC();

    fuzzcheck.root_module.addCMacro("SQLITE_OSS_FUZZ", "");
    fuzzcheck.root_module.addCMacro("SQLITE_NO_SYNC", "1");
    fuzzcheck.root_module.addCMacro("SQLITE_OMIT_LOAD_EXTENSION", "1");

    const testfixture = b.addExecutable(.{
        .name = "testfixture",
        .root_source_file = null,
        .target = target,
        .optimize = .ReleaseFast,
    });
    testfixture.linkLibC();
    testfixture.linkLibrary(
        addLibsql(b, .{
            .target = options.target,
            .optimize = options.optimize,
            .rtree = options.rtree,
            .session = options.session,
            .fts3 = options.fts3,
            .fts5 = options.fts5,
            .geopoly = options.geopoly,
            .icu = options.icu,
            .wasm_runtime = options.wasm_runtime,
            .@"test" = true,
        }),
    );
    testfixture.addIncludePath(b.path("src/"));
    testfixture.addCSourceFiles(.{ .files = sources.testfixture, .flags = cflags(b, &.{}) });

    testfixture.root_module.addCMacro("SQLITE_HAVE_ZLIB", "1");
    testfixture.root_module.addCMacro("SQLITE_TEST", "1");

    testfixture.root_module.addCMacro("TCLSH_INIT_PROC", "sqlite3TestInit");
    testfixture.root_module.addCMacro("SQLITE_ENABLE_STMTVTAB", "1");
    testfixture.root_module.addCMacro("SQLITE_ENABLE_DBPAGE_VTAB", "1");
    testfixture.root_module.addCMacro("SQLITE_ENABLE_BYTECODE_VTAB", "1");
    testfixture.root_module.addCMacro("SQLITE_SERIES_CONSTRAINT_VERIFY", "1");
    testfixture.root_module.addCMacro("SQLITE_CKSUMVFS_STATIC", "1");
    testfixture.root_module.addCMacro("SQLITE_DEFAULT_PAGE_SIZE", "1024");

    if (options.icu) testfixture.root_module.addCMacro("SQLITE_ENABLE_ICU", "1");
    if (options.fts3) testfixture.root_module.addCMacro("SQLITE_ENABLE_FTS3", "1");
    if (options.session) {
        testfixture.root_module.addCMacro("SQLITE_ENABLE_SESSION", "1");
        testfixture.root_module.addCMacro("SQLITE_ENABLE_PREUPDATE_HOOK", "1");
    }

    testfixture.linkLibrary(zlib.artifact("z"));
    testfixture.linkSystemLibrary("tcl");

    {
        const run = b.addRunArtifact(testfixture);
        if (b.args) |args| {
            run.addArgs(args);
        } else {
            run.addArgs(&.{ "test/testrunner.tcl", "veryquick" });
        }

        const rust_suite = b.addSystemCommand(&.{ "cargo", "test" });
        rust_suite.setCwd(b.path("test/rust_suite"));
        rust_suite.step.dependOn(&run.step);

        const step = b.step(
            "test",
            "Run testfixture (default: testrunner.tcl veryquick)",
        );
        step.dependOn(&run.step);
        step.dependOn(&rust_suite.step);
    }

    {
        const run = b.addRunArtifact(fuzzcheck);
        if (b.args) |args| {
            run.addArgs(args);
        } else {
            inline for (&.{
                "test/fuzzdata1.db",
                "test/fuzzdata2.db",
                "test/fuzzdata3.db",
                "test/fuzzdata4.db",
                "test/fuzzdata5.db",
                "test/fuzzdata6.db",
                "test/fuzzdata7.db",
                "test/fuzzdata8.db",
            }) |path| {
                run.addFileArg(b.path(path));
            }
        }

        const step = b.step(
            "fuzzcheck",
            "Run fuzzcheck (default: test/fuzzdata[1..=8].db)",
        );
        step.dependOn(&run.step);
    }

    b.getInstallStep().dependOn(&b.addInstallArtifact(testfixture, .{}).step);
    b.getInstallStep().dependOn(&b.addInstallArtifact(fuzzcheck, .{}).step);
    b.getInstallStep().dependOn(&b.addInstallArtifact(libsql, .{}).step);
}
