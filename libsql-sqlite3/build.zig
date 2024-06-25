const std = @import("std");

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
        return self;
    }

    pub fn make(step: *std.Build.Step, _: std.Progress.Node) !void {
        const self: *Debug = @fieldParentPtr("step", step);

        const b = self.step.owner;

        std.debug.print("{s}", .{self.path.getPath(b)});
    }
};

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lemon = b.addExecutable(.{
        .name = "lemon",
        .root_source_file = null,
        .target = target,
        .optimize = .ReleaseFast,
    });
    lemon.addCSourceFile(.{ .file = b.path("tool/lemon.c") });
    lemon.linkLibC();

    {
        const run = b.addRunArtifact(lemon);
        run.addArg("-DSQLITE_ENABLE_MATH_FUNCTIONS");
        const parse = run.addPrefixedOutputDirectoryArg("-d", "parser");
        run.addFileArg(b.path("src/parse.y"));

        const debug = Debug.create(b, parse);
        debug.step.dependOn(&run.step);

        const step = b.step("gen-parse", "Generate parse files");
        step.dependOn(&debug.step);
        step.dependOn(&run.step);
    }

    const sources = .{
        .sqlite3 = &.{
            "src/alter.c",
            "src/analyze.c",
            "src/attach.c",
            "src/auth.c",
            "src/backup.c",
            "src/bitvec.c",
            "src/btmutex.c",
            "src/btree.c",
            "src/build.c",
            "src/callback.c",
            "src/complete.c",
            "src/ctime.c",
            "src/date.c",
            "src/dbpage.c",
            "src/dbstat.c",
            "src/delete.c",
            "src/expr.c",
            "src/fault.c",
            "src/fkey.c",
            "src/func.c",
            "src/global.c",
            "src/hash.c",
            "src/insert.c",
            "src/json.c",
            "src/legacy.c",
            "src/loadext.c",
            "src/main.c",
            "src/malloc.c",
            "src/mem0.c",
            "src/mem1.c",
            "src/mem2.c",
            "src/mem3.c",
            "src/mem5.c",
            "src/memdb.c",
            "src/memjournal.c",
            "src/mutex.c",
            "src/mutex_noop.c",
            "src/mutex_unix.c",
            "src/mutex_w32.c",
            "src/notify.c",
            "src/os.c",
            "src/os_kv.c",
            "src/os_unix.c",
            "src/os_win.c",
            "src/pager.c",
            "src/pcache.c",
            "src/pcache1.c",
            "src/pragma.c",
            "src/prepare.c",
            "src/printf.c",
            "src/random.c",
            "src/resolve.c",
            "src/rowset.c",
            "src/select.c",
            "src/status.c",
            "src/table.c",
            "src/threads.c",
            "src/tokenize.c",
            "src/treeview.c",
            "src/trigger.c",
            "src/utf.c",
            "src/update.c",
            "src/upsert.c",
            "src/util.c",
            "src/vacuum.c",
            "src/vdbe.c",
            "src/vdbeapi.c",
            "src/vdbeaux.c",
            "src/vdbeblob.c",
            "src/vdbemem.c",
            "src/vdbesort.c",
            "src/vdbetrace.c",
            "src/vdbevtab.c",
            "src/vtab.c",
            "src/wal.c",
            "src/walker.c",
            "src/where.c",
            "src/wherecode.c",
            "src/whereexpr.c",
            "src/window.c",
        },
        .extensions = .{
            .fts3 = &.{
                "ext/fts3/fts3.c",
                "ext/fts3/fts3.h",
                "ext/fts3/fts3Int.h",
                "ext/fts3/fts3_aux.c",
                "ext/fts3/fts3_expr.c",
                "ext/fts3/fts3_hash.c",
                "ext/fts3/fts3_hash.h",
                "ext/fts3/fts3_icu.c",
                "ext/fts3/fts3_porter.c",
                "ext/fts3/fts3_snippet.c",
                "ext/fts3/fts3_tokenizer.h",
                "ext/fts3/fts3_tokenizer.c",
                "ext/fts3/fts3_tokenizer1.c",
                "ext/fts3/fts3_tokenize_vtab.c",
                "ext/fts3/fts3_unicode.c",
                "ext/fts3/fts3_unicode2.c",
                "ext/fts3/fts3_write.c",
            },
            .icu = &.{
                "ext/icu/sqliteicu.h",
                "ext/icu/icu.c",
            },
            .rtree = &.{
                "ext/rtree/rtree.h",
                "ext/rtree/rtree.c",
                "ext/rtree/geopoly.c",
            },
            .session = &.{
                "ext/session/sqlite3session.c",
                "ext/session/sqlite3session.h",
            },
            .auth = &.{
                "ext/userauth/userauth.c",
                "ext/userauth/sqlite3userauth.h",
            },
            .rbu = &.{
                "ext/rbu/sqlite3rbu.h",
                "ext/rbu/sqlite3rbu.c",
            },
            .stmt = &.{
                "ext/misc/stmt.c",
            },
            .wasm = &.{
                "ext/udf/wasmedge_bindings.c",
            },
        },
    };

    const sqlite_header = b.addConfigHeader(
        .{
            .include_path = "sqlite3.h",
            .style = .{ .cmake = b.path("src/sqlite.h.in") },
        },
        .{
            .sqlite_version = "3.44.0",
            .sqlite_version_number = 3044000,
            .sqlite_source_id = "2023-11-01 11:23:50 17129ba1ff7f0daf37100ee82d507aef7827cf38de1866e2633096ae6ad8alt1",
            .libsql_version = "0.2.3",
        },
    );

    const sqlite_cfg = b.addConfigHeader(
        .{
            .include_path = "sqlite_cfg.h",
            .style = .{ .autoconf = b.path("sqlite_cfg.h.in") },
        },
        .{
            .HAVE_DLFCN_H = null,

            // Define to 1 if you have the `fdatasync' function.
            .HAVE_FDATASYNC = 1,

            // Define to 1 if you have the `gmtime_r' function.
            .HAVE_GMTIME_R = 1,

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

            // Define to 1 if you have the `isnan' function.
            .HAVE_ISNAN = 1,

            // Define to 1 if you have the `localtime_r' function.
            .HAVE_LOCALTIME_R = 1,

            // Define to 1 if you have the `localtime_s' function.
            .HAVE_LOCALTIME_S = 1,

            // Define to 1 if you have the <malloc.h> header file.
            .HAVE_MALLOC_H = 1,

            // Define to 1 if you have the `malloc_usable_size' function.
            .HAVE_MALLOC_USABLE_SIZE = 1,

            // Define to 1 if you have the <memory.h> header file.
            .HAVE_MEMORY_H = 1,

            // Define to 1 if you have the `pread' function.
            .HAVE_PREAD = 1,
            .HAVE_PREAD64 = 1,
            .HAVE_PWRITE = 1,
            .HAVE_PWRITE64 = 1,

            // Define to 1 if you have the `strchrnul' function.
            .HAVE_STRCHRNUL = 1,

            // Define to 1 if you have the <strings.h> header file.
            .HAVE_STRINGS_H = 1,

            // Define to 1 if you have the <string.h> header file.
            .HAVE_STRING_H = 1,

            // Define to 1 if you have the <sys/stat.h> header file.
            .HAVE_SYS_STAT_H = 1,

            // Define to 1 if you have the <sys/types.h> header file.
            .HAVE_SYS_TYPES_H = 1,


            // Define to 1 if you have the <unistd.h> header file.
            .HAVE_UNISTD_H = 1,

            // Define to 1 if you have the `usleep' function.
            .HAVE_USLEEP = 1,

            // Define to 1 if you have the `utime' function.
            .HAVE_UTIME = 1,

            // Define to 1 if you have the <zlib.h> header file.
            .HAVE_ZLIB_H = 1,

            // Define to the sub-directory in which libtool stores uninstalled libraries.
            .LT_OBJDIR = null,

            // Define to the address where bug reports for this package should be sent.
            .PACKAGE_BUGREPORT = "",

            // Define to the full name of this package.
            .PACKAGE_NAME = "",

            // Define to the full name and version of this package.
            .PACKAGE_STRING = "",

            // Define to the one symbol short name of this package.
            .PACKAGE_TARNAME = "",

            // Define to the home page for this package.
            .PACKAGE_URL = "",

            // Define to the version of this package.
            .PACKAGE_VERSION = "",

            // Number of bits in a file offset, on hosts where this is settable.
            ._FILE_OFFSET_BITS = null,

            // Define for large files, on AIX-style hosts.
            ._LARGE_FILES = null,
        },
    );

    const debug = Debug.create(b, sqlite_header.getOutput());

    const sqlite3 = b.addStaticLibrary(.{
        .name = "sqlite3",
        .root_source_file = null,
        .target = target,
        .optimize = optimize,
    });
    sqlite3.addIncludePath(b.path("src/"));
    sqlite3.addIncludePath(b.path("."));
    sqlite3.addConfigHeader(sqlite_header);
    sqlite3.addConfigHeader(sqlite_cfg);
    sqlite3.addCSourceFiles(.{ .files = sources.sqlite3, .flags = &.{ "-g", "-DLIBSQL_ENABLE_WASM_RUNTIME" } });
    sqlite3.linkLibC();

    debug.step.dependOn(&sqlite_header.step);

    const install = b.addInstallArtifact(sqlite3, .{});

    b.getInstallStep().dependOn(&install.step);
    b.getInstallStep().dependOn(&debug.step);
}
