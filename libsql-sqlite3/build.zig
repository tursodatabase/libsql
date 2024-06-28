const std = @import("std");
const crab = @import("build.crab");

pub const Debug = struct {
    step: std.Build.Step,
    path: std.Build.LazyPath,

    pub fn create(b: *std.Build, path: std.Build.LazyPath) *Debug {
        const self = b.allocator.create(Debug) catch @panic("OOM");
        self.* = .{
            .step = std.Build.Step.init(.{
                .id = .custom,
                .name = "debug path",
                .owner = b,
                .makeFn = make,
            }),
            .path = path,
        };

        path.addStepDependencies(&self.step);

        return self;
    }

    pub fn make(step: *std.Build.Step, _: std.Progress.Node) !void {
        const self: *Debug = @fieldParentPtr("step", step);

        const b = self.step.owner;

        std.debug.print("welp {s}\n", .{self.path.getPath(b)});
    }
};

pub const Amalgamation = struct {
    step: std.Build.Step,
    lazy_paths: std.ArrayList(std.Build.LazyPath),
    output_list: std.Build.GeneratedFile,
    basename: []const u8,

    pub fn create(b: *std.Build, basename: []const u8, lazy_paths: []const std.Build.LazyPath) *Amalgamation {
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

        var list = std.ArrayList(std.Build.LazyPath).init(b.allocator);

        for (lazy_paths) |lp| {
            list.append(lp) catch @panic("OOM");
            lp.addStepDependencies(&self.step);
        }

        self.lazy_paths = list;

        return self;
    }

    pub fn getOutput(self: *const Amalgamation) std.Build.LazyPath {
        return .{ .generated = .{ .file = &self.output_list } };
    }

    pub fn make(step: *std.Build.Step, prog_node: std.Progress.Node) !void {
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
        // try man.writeManifest();
    }
};

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const sources = .{
        .sqlite3 = &.{
            "src/alter.c",      "src/analyze.c",    "src/attach.c",     "src/auth.c",
            "src/backup.c",     "src/bitvec.c",     "src/btmutex.c",    "src/btree.c",
            "src/build.c",      "src/callback.c",   "src/complete.c",   "src/ctime.c",
            "src/date.c",       "src/dbpage.c",     "src/dbstat.c",     "src/delete.c",
            "src/expr.c",       "src/fault.c",      "src/fkey.c",       "src/func.c",
            "src/global.c",     "src/hash.c",       "src/insert.c",     "src/json.c",
            "src/legacy.c",     "src/loadext.c",    "src/main.c",       "src/malloc.c",
            "src/mem0.c",       "src/mem1.c",       "src/mem2.c",       "src/mem3.c",
            "src/mem5.c",       "src/memdb.c",      "src/memjournal.c", "src/mutex.c",
            "src/mutex_noop.c", "src/mutex_unix.c", "src/mutex_w32.c",  "src/notify.c",
            "src/os.c",         "src/os_kv.c",      "src/os_unix.c",    "src/os_win.c",
            "src/pager.c",      "src/pcache.c",     "src/pcache1.c",    "src/pragma.c",
            "src/prepare.c",    "src/printf.c",     "src/random.c",     "src/resolve.c",
            "src/rowset.c",     "src/select.c",     "src/status.c",     "src/table.c",
            "src/threads.c",    "src/tokenize.c",   "src/treeview.c",   "src/trigger.c",
            "src/utf.c",        "src/update.c",     "src/upsert.c",     "src/util.c",
            "src/vacuum.c",     "src/vdbe.c",       "src/vdbeapi.c",    "src/vdbeaux.c",
            "src/vdbeblob.c",   "src/vdbemem.c",    "src/vdbesort.c",   "src/vdbetrace.c",
            "src/vdbevtab.c",   "src/vtab.c",       "src/wal.c",        "src/walker.c",
            "src/where.c",      "src/wherecode.c",  "src/whereexpr.c",  "src/window.c",
        },
        .extensions = .{
            .fts3 = &.{
                "ext/fts3/fts3.c",            "ext/fts3/fts3_aux.c",
                "ext/fts3/fts3_expr.c",       "ext/fts3/fts3_hash.c",
                "ext/fts3/fts3_icu.c",        "ext/fts3/fts3_porter.c",
                "ext/fts3/fts3_snippet.c",    "ext/fts3/fts3_tokenizer.c",
                "ext/fts3/fts3_tokenizer1.c", "ext/fts3/fts3_tokenize_vtab.c",
                "ext/fts3/fts3_unicode.c",    "ext/fts3/fts3_unicode2.c",
                "ext/fts3/fts3_write.c",
            },
            .icu = &.{"ext/icu/icu.c"},
            .rtree = &.{ "ext/rtree/rtree.c", "ext/rtree/geopoly.c" },
            .session = &.{"ext/session/sqlite3session.c"},
            .auth = &.{"ext/userauth/userauth.c"},
            .rbu = &.{"ext/rbu/sqlite3rbu.c"},
            .stmt = &.{"ext/misc/stmt.c"},
            .wasm = &.{"ext/udf/wasmedge_bindings.c"},
        },
    };

    const sqlite_header_base = b.addConfigHeader(
        .{
            .include_path = "sqlite3.h",
            .style = .{ .cmake = b.path("src/sqlite.h.in") },
        },
        .{
            .libsql_version = "0.2.3",
            .sqlite_version = "3.44.0",
            .sqlite_version_number = 3044000,
            .sqlite_source_id = "2023-11-01 11:23:50 17129ba1ff7f0daf37100ee82d507aef7827cf38de1866e2633096ae6ad8alt1",
        },
    );

    const sqlite_cfg = b.addConfigHeader(
        .{
            .include_path = "sqlite_cfg.h",
            .style = .{ .autoconf = b.path("sqlite_cfg.h.in") },
        },
        .{
            .HAVE_DLFCN_H = null,
            .HAVE_FDATASYNC = 1, // Define to 1 if you have the `fdatasync' function.
            .HAVE_GMTIME_R = 1, // Define to 1 if you have the `gmtime_r' function.

            .STDC_HEADERS = 1,
            .HAVE_STDINT_H = 1,
            .HAVE_STDLIB_H = 1,

            .HAVE_INT16_T = 1,
            .HAVE_INT32_T = 1,
            .HAVE_INT64_T = 1,
            .HAVE_INT8_T = 1,
            .HAVE_INTPTR_T = 1,
            .HAVE_INTTYPES_H = 1,

            .HAVE_UINT16_T = 1,
            .HAVE_UINT32_T = 1,
            .HAVE_UINT64_T = 1,
            .HAVE_UINT8_T = 1,
            .HAVE_UINTPTR_T = 1,

            .HAVE_ISNAN = 1, // Define to 1 if you have the `isnan' function.
            .HAVE_LOCALTIME_R = 1, // Define to 1 if you have the `localtime_r' function.
            .HAVE_LOCALTIME_S = 1, // Define to 1 if you have the `localtime_s' function.
            .HAVE_MALLOC_H = 1, // Define to 1 if you have the <malloc.h> header file.
            .HAVE_MALLOC_USABLE_SIZE = 1, // Define to 1 if you have the `malloc_usable_size' function.
            .HAVE_MEMORY_H = 1, // Define to 1 if you have the <memory.h> header file.

            .HAVE_PREAD = 1, // Define to 1 if you have the `pread' function.
            .HAVE_PREAD64 = 1,
            .HAVE_PWRITE = 1,
            .HAVE_PWRITE64 = 1,

            .HAVE_STRCHRNUL = 1, // Define to 1 if you have the `strchrnul' function.
            .HAVE_STRINGS_H = 1, // Define to 1 if you have the <strings.h> header file.
            .HAVE_STRING_H = 1, // Define to 1 if you have the <string.h> header file.
            .HAVE_SYS_STAT_H = 1, // Define to 1 if you have the <sys/stat.h> header file.
            .HAVE_SYS_TYPES_H = 1, // Define to 1 if you have the <sys/types.h> header file.
            .HAVE_UNISTD_H = 1, // Define to 1 if you have the <unistd.h> header file.
            .HAVE_USLEEP = 1, // Define to 1 if you have the `usleep' function.
            .HAVE_UTIME = 1, // Define to 1 if you have the `utime' function.
            .HAVE_ZLIB_H = 1, // Define to 1 if you have the <zlib.h> header file.

            .LT_OBJDIR = null, // Define to the sub-directory in which libtool stores uninstalled libraries.
            .PACKAGE_BUGREPORT = "", // Define to the address where bug reports for this package should be sent.
            .PACKAGE_NAME = "", // Define to the full name of this package.
            .PACKAGE_STRING = "", // Define to the full name and version of this package.
            .PACKAGE_TARNAME = "", // Define to the one symbol short name of this package.
            .PACKAGE_URL = "", // Define to the home page for this package.
            .PACKAGE_VERSION = "", // Define to the version of this package.
            ._FILE_OFFSET_BITS = null, // Number of bits in a file offset, on hosts where this is settable.
            ._LARGE_FILES = null, // Define for large files, on AIX-style hosts.
        },
    );

    const lemon = b.addExecutable(.{
        .name = "lemon",
        .root_source_file = null,
        .target = target,
        .optimize = .ReleaseFast,
    });
    lemon.addCSourceFile(.{ .file = b.path("tool/lemon.c") });
    lemon.linkLibC();

    const mkkeywordhash = b.addExecutable(.{
        .name = "mkkeywordhash",
        .root_source_file = null,
        .target = target,
        .optimize = .ReleaseFast,
    });
    mkkeywordhash.addCSourceFile(.{ .file = b.path("tool/mkkeywordhash.c") });
    mkkeywordhash.linkLibC();

    var parser = parser: {
        const run = b.addRunArtifact(lemon);
        run.setCwd(b.path("tool/"));
        run.addArg("-DSQLITE_ENABLE_MATH_FUNCTIONS");
        const parser = run.addPrefixedOutputDirectoryArg("-d", ".");
        run.addArg("-S");
        run.addFileArg(b.path("src/parse.y"));
        break :parser parser;
    };

    var wasm_runtime = b.option(bool, "wasm-runtime", "Enable wasm runtime (default: false)") orelse false;
    var icu = b.option(bool, "icu", "Enable icu extension (default: false)") orelse false;

    var keywordhash = b.addRunArtifact(mkkeywordhash);

    const sqlite_header = Amalgamation.create(
        b,
        "sqlite3.h",
        &.{
            sqlite_header_base.getOutput(),
            b.path("src/page_header.h"),
            b.path("src/wal.h"),
        },
    );

    // const debug = Debug.create(b, parser);
    // b.getInstallStep().dependOn(&debug.step);

    const parser_vdbe = Amalgamation.create(b, "parse_vbde", &.{
        parser.path(b, "parse.h"),
        b.path("src/vdbe.c"),
    });

    const opcode_h = b.addSystemCommand(&.{"tclsh"});
    opcode_h.addFileArg(b.path("tool/mkopcodeh.tcl"));
    opcode_h.setStdIn(.{ .lazy_path = parser_vdbe.getOutput() });

    const opcode_c = b.addSystemCommand(&.{"tclsh"});
    opcode_c.addFileArg(b.path("tool/mkopcodec.tcl"));
    opcode_c.addFileArg(opcode_h.captureStdOut());

    const generated = b.addWriteFiles();
    _ = generated.addCopyFile(keywordhash.captureStdOut(), "keywordhash.h");
    _ = generated.addCopyFile(opcode_h.captureStdOut(), "opcodes.h");
    _ = generated.addCopyFile(opcode_c.captureStdOut(), "opcodes.c");

    const flags = &.{
        "-g",
        if (wasm_runtime) "-DLIBSQL_ENABLE_WASM_RUNTIME" else "",
    };

    const sqlite3 = b.addStaticLibrary(.{
        .name = "sqlite3",
        .root_source_file = null,
        .target = target,
        .optimize = optimize,
    });
    sqlite3.addObject(object: {
        const object = b.addObject(.{
            .name = "sqlite3",
            .target = target,
            .optimize = optimize,
        });
        object.linkLibC();
        object.addIncludePath(b.path("src/"));
        object.addIncludePath(generated.getDirectory());
        object.addIncludePath(parser);
        object.addIncludePath(sqlite_header.getOutput().dirname());
        object.addConfigHeader(sqlite_cfg);
        object.addCSourceFile(.{
            .file = generated.getDirectory().path(b, "opcodes.c"),
            .flags = flags,
        });
        object.addCSourceFile(.{ .file = parser.path(b, "parse.c"), .flags = flags });
        object.addCSourceFiles(.{ .files = sources.sqlite3, .flags = flags });

        if (icu) {
        }

        if (wasm_runtime) {
            _ = generated.addCopyFile(b.path("ext/udf/wasm_bindings.h"), "ext/udf/wasm_bindings.h");

            const libsql_wasm = crab.addCargoBuildWithUserOptions(b, .{
                .name = "liblibsql_wasm.a",
                .manifest_path = b.path("crates/wasmtime-bindings/Cargo.toml"),
                .cargo_args = &.{
                    "--release",
                    "--lib",
                },
            }, .{
                .target = target,
                .optimize = .ReleaseSafe,
            });

            object.addCSourceFile(.{
                .file = b.path(sources.wasm),
                .flags = flags ++ &.{"-DSQLITE_CORE"},
            });
            object.addObjectFile(libsql_wasm);
        }

        break :object object;
    });

    const install = b.addInstallArtifact(sqlite3, .{});

    b.getInstallStep().dependOn(&install.step);
}
