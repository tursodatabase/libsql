/*
** 2024-09-23
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This app's single purpose is to emit parts of the Makefile code for
** sqlite3's canonical WASM build. The main motivation is to generate
** code which "could" be created via GNU Make's eval command but is
** highly illegible when constructed that way. Attempts to write this
** app in Bash and TCL have suffered from the problem that those
** languages require escaping $ symbols, making the resulting script
** code as illegible as the eval spaghetti we want to get away
** from. Maintaining it in C is, somewhat surprisingly, _slightly_
** less illegible than writing it in bash, tcl, or native Make code.
**
** The emitted makefile code is not standalone - it depends on
** variables and $(call)able functions from the main makefile.
*/

#undef NDEBUG
#define DEBUG 1
#include <assert.h>
#include <stdio.h>
#include <string.h>

#define pf printf
#define ps puts

/* Separator to help eyeballs find the different output sections */
#define zBanner \
  "\n########################################################################\n"

/*
** Flags for use with BuildDef::flags.
**
** Maintenance reminder: do not combine flags within this enum,
** e.g. F_BUNDLER_FRIENDLY=0x02|F_ESM, as that will lead
** to breakage in some of the flag checks.
*/
enum BuildDefFlags {
  /* Indicates an ESM module build. */
  F_ESM              = 0x01,
  /* Indicates a "bundler-friendly" build mode. */
  F_BUNDLER_FRIENDLY = 1<<1,
  /* Indicates that this build is unsupported. Such builds are not
  ** added to the 'all' target. The unsupported builds exist primarily
  ** for experimentation's sake. */
  F_UNSUPPORTED      = 1<<2,
  /* Elide this build from the 'all' target. */
  F_NOT_IN_ALL       = 1<<3,
  /* If it's a 64-bit build. */
  F_64BIT            = 1<<4,
  /* Indicates a node.js-for-node.js build (untested and
  ** unsupported). */
  F_NODEJS           = 1<<5,
  /* Indicates a wasmfs build (untested and unsupported). */
  F_WASMFS           = 1<<6,

  /*
  ** Which compiled files from $(dir.dout)/buildName/*.{js,mjs,wasm}
  ** to copy to $(dir.dout) after creating them. This should only be
  ** applied to builds which result in end-user deliverables.  Some
  ** builds, like the bundler-friendly ones, are a hybrid: we keep
  ** only their JS file and patch their JS to use the WASM file from a
  ** canonical build which uses that same WASM file. Reusing X.wasm
  ** that way can only work for builds which are processed identically
  ** by Emscripten. For a given set of C flags (as opposed to
  ** JS-influencing flags), all builds of X.js and Y.js will produce
  ** identical X.wasm and Y.wasm files. Their JS files may well
  ** differ, however.
  */
  CP_JS              = 1 << 30,
  CP_WASM            = 1 << 31,
  CP_ALL             = CP_JS | CP_WASM
};

/*
** Info needed for building one concrete JS/WASM combination..
**
** Notes about Emscripten builds...
**
** When emcc processes X.js it also generates X.wasm and hard-codes
** the name "X.wasm" into the JS file (it has to - there's no reliable
** way to derive that name at runtime for certain modes of loading the
** WASM file). Because we only need two sqlite3.wasm files (one each
** for 32- and 64-bit), the build then copies just those into the
** final build directory $(dir.dout).
**
** To keep parallel builds from stepping on each other, each distinct
** build goes into its own subdir $(dir.dout.BuildName)[^1], i.e.
** $(dir.dout)/BuildName.  Builds which produce deliverables we'd like
** to keep/distribute copy their final results into the build dir
** $(dir.dout). See the notes for the CP_JS enum entry for more
** details on that.
**
** The final result of each build is a pair of JS/WASM files, but
** getting there requires generation of several files, primarily as
** inputs for specific Emscripten flags:
**
** --pre-js = file gets injected after Emscripten's earliest starting
** point, enabling limited customization of Emscripten's
** behavior. This code lives/runs within the generated sqlite3InitModule().
**
** --post-js = gets injected after Emscripten's main work, but still
** within the body of sqlite3InitModule().
**
** --extern-pre-js = gets injected before sqlite3InitModule(), in the
** global scope. We inject the license and version info here.
**
** --extern-post-js = gets injected immediately after
** sqlite3InitModule(), in the global scope. In this step we replace
** sqlite3InitModule() with a slightly customized, the main purpose of
** which is to (A) give us (not Emscripten) control over the arguments
** it accepts and (B) to run the library bootstrap step.
**
** Then there's sqlite3-api.BuildName.js, which is the entire SQLite3
** JS API (generated from the list defined in $(sqlite3-api.jses)). It
** gets sandwitched inside --post-js.
**
** Each of those inputs has to be generated before passing them on to
** Emscripten so that any build-specific capabilities can get filtered
** in or out (using ./c-pp.c).
**
** [^1]: The legal BuildNames are in this file's BuildDef_map macro.
*/
struct BuildDef {
  /*
  ** Base name of output JS and WASM files. The X part of X.js and
  ** X.wasm.
  */
  const char *zBaseName;
  /*
  ** A glyph to use in log messages for this build, intended to help
  ** the eyes distinguish the build lines more easily in parallel
  ** builds.
  **
  ** The convention for 32- vs 64-bit pairs is to give them similar
  ** emoji, e.g. a cookie for 32-bit and a donut or cake for 64.
  ** Alternately, the same emoji a "64" suffix, excep that that throws
  ** off the output alignment in parallel builds ;).
  */
  const char *zEmo;
  /*
  ** If the build needs its x.wasm renamed in its x.{js,mjs} then this
  ** must hold the base name to rename it to.  Typically "sqlite3" or
  ** "sqlite3-64bit". This is the case for builds which are named
  ** something like sqlite3-foo-bar but can use the vanilla
  ** sqlite3.wasm file. In such cases we don't need the extra
  ** sqlite3-foo-bar.wasm which Emscripten (necessarily) creates when
  ** compiling the module, so we patch (at build-time) the JS file to
  ** use this name instead sqlite3-foo-bar.
  */
  const char *zDotWasm;
  const char *zCmppD;     /* Extra -D... flags for c-pp */
  const char *zEmcc;      /* Full flags for emcc. Normally NULL for default. */
  const char *zEmccExtra; /* Extra flags for emcc */
  const char *zDeps;      /* Extra deps */
  const char *zEnv;       /* emcc -sENVIRONMENT=... value */
  /*
  ** Makefile code "ifeq (...)". If set, this build is enclosed in a
  ** $zIfCond/endif block.
  */
  const char *zIfCond;    /* makefile "ifeq (...)" or similar */
  int flags;              /* Flags from BuildDefFlags */
};
typedef struct BuildDef BuildDef;

/*
** List of distinct library builds. Each one has to be set up in
** oBuildDefs. See the next comment block.
**
** Many makefile vars use these identifiers for naming stuff, e.g.:
**
**  out.NAME.js   = output JS file for the build named NAME
**  out.NAME.wasm = output WASM file for the build named NAME
**  logtag.NAME   = Used for decorating log output
**
** etc.
***/
#define BuildDefs_map(E)  \
  E(vanilla) E(vanilla64) \
  E(esm)     E(esm64)     \
  E(bundler) E(bundler64) \
  E(speedtest1) E(speedtest164) \
  E(node)    E(node64)    \
  E(wasmfs)

/*
** The set of WASM builds for the library (as opposed to the apps
** (fiddle, speedtest1)). Their order in BuildDefs_map is mostly
** insignificant, but some makefile vars used by some builds are set
** up by prior builds. Because of that, the (sqlite3, vanilla),
** (sqlite3, esm), and (sqlite3, bundler-friendly) builds should be
** defined first (in that order).
*/
struct BuildDefs {
#define E(N) BuildDef N;
  BuildDefs_map(E)
#undef E
};
typedef struct BuildDefs BuildDefs;

const BuildDefs oBuildDefs = {
  /*
  ** The canonical build, against which all others are compared and
  ** contrasted.  This is the one we post downloads for.
  **
  ** This one's zBaseName and zEnv MUST be non-NULL so it can be used
  ** as a default for all others
  */
  .vanilla = {
    .zEmo        = "🍦",
    .zBaseName   = "sqlite3",
    .zDotWasm    = 0,
    .zCmppD      = 0,
    .zEmcc       = 0,
    .zEmccExtra  = 0,
    .zEnv        = "web,worker",
    .zDeps       = 0,
    .zIfCond     = 0,
    .flags       = CP_ALL
  },

  /* The canonical build in 64-bit. */
  .vanilla64 = {
    .zEmo        = "🍨",
    .zBaseName   = "sqlite3-64bit",
    .zDotWasm    = 0,
    .zCmppD      = 0,
    .zEmcc       = 0,
    .zEmccExtra  = "-sMEMORY64=1 -sWASM_BIGINT=1",
    .zEnv        = 0,
    .zDeps       = 0,
    .zIfCond     = 0,
    .flags       = CP_ALL | F_64BIT
  },

  /* The canonical esm build. */
  .esm = {
    .zEmo        = "🍬",
    .zBaseName   = "sqlite3",
    .zDotWasm    = 0,
    .zCmppD      = "-Dtarget:es6-module",
    .zEmcc       = 0,
    .zEmccExtra  = 0,
    .zEnv        = 0,
    .zDeps       = 0,
    .zIfCond     = 0,
    .flags       = CP_JS | F_ESM
  },

  /* The canonical esm build in 64-bit. */
  .esm64 = {
    .zEmo        = "🍫",
    .zBaseName   = "sqlite3-64bit",
    .zDotWasm    = 0,
    .zCmppD      = "-Dtarget:es6-module",
    .zEmcc       = 0,
    .zEmccExtra  = "-sMEMORY64=1 -sWASM_BIGINT=1",
    .zEnv        = 0,
    .zDeps       = 0,
    .zIfCond     = 0,
    .flags       = CP_JS | F_ESM | F_64BIT
  },

 /* speedtest1, our primary benchmarking tool */
  .speedtest1 = {
    .zEmo        = "🛼",
    .zBaseName   = "speedtest1",
    .zDotWasm    = 0,
    .zCmppD      = 0,
    .zEmcc       =
    "$(emcc.speedtest1)"
    " $(emcc.speedtest1.common)"
    " $(pre-post.speedtest1.flags)"
    " $(cflags.common)"
    " -DSQLITE_SPEEDTEST1_WASM"
    " $(SQLITE_OPT)"
    " -USQLITE_WASM_BARE_BONES"
    " -USQLITE_C -DSQLITE_C=$(sqlite3.canonical.c)"
    " $(speedtest1.exit-runtime0)"
    " $(speedtest1.c.in)"
    " -lm",
    .zEmccExtra  = 0,
    .zEnv        = 0,
    .zDeps       =
    "$(speedtest1.c.in)"
    " $(EXPORTED_FUNCTIONS.speedtest1)",
    .zIfCond     = 0,
    .flags       = CP_ALL
  },

 /* speedtest1 64-bit */
  .speedtest164 = {
    .zEmo        = "🛼64",
    .zBaseName   = "speedtest1-64bit",
    .zDotWasm    = 0,
    .zCmppD      = 0,
    .zEmcc       =
    "$(emcc.speedtest1)"
    " $(emcc.speedtest1.common)"
    " -sMEMORY64=1 -sWASM_BIGINT=1"
    " $(pre-post.speedtest164.flags)"
    " $(cflags.common)"
    " -DSQLITE_SPEEDTEST1_WASM"
    " $(SQLITE_OPT)"
    " -USQLITE_WASM_BARE_BONES"
    " -USQLITE_C -DSQLITE_C=$(sqlite3.canonical.c)"
    " $(speedtest1.exit-runtime0)"
    " $(speedtest1.c.in)"
    " -lm",
    .zEmccExtra  = 0,
    .zEmccExtra  = 0,
    .zEnv        = 0,
    .zDeps       =
    "$(speedtest1.c.in)"
    " $(EXPORTED_FUNCTIONS.speedtest1)",
    .zIfCond     = 0,
    .flags       = CP_ALL | F_NOT_IN_ALL
  },

  /*
  ** Core bundler-friendly build. Untested and "not really" supported,
  ** but required by the downstream npm subproject.
  **
  ** Testing these requires special-purpose node-based tools and
  ** custom test apps, none of which we have/use. So we can pass them
  ** off as-is to the npm subproject and they spot failures pretty
  ** quickly ;).
  */
  .bundler = {
    .zEmo        = "👛",
    .zBaseName   = "sqlite3-bundler-friendly",
    .zDotWasm    = "sqlite3",
    .zCmppD      = "$(c-pp.D.esm) -Dtarget:es6-bundler-friendly",
    .zEmcc       = 0,
    .zEmccExtra  = 0,
    .zEnv        = 0,
    .zDeps       = 0,
    .zIfCond     = 0,
    .flags       = CP_JS | F_BUNDLER_FRIENDLY | F_ESM
    //| F_NOT_IN_ALL
  },

  /* 64-bit bundler-friendly. */
  .bundler64 = {
    .zEmo        = "📦",
    .zBaseName   = "sqlite3-bundler-friendly-64bit",
    .zDotWasm    = "sqlite3-64bit",
    .zCmppD      = "$(c-pp.D.bundler)",
    .zEmcc       = 0,
    .zEmccExtra  = "-sMEMORY64=1",
    .zEnv        = 0,
    .zDeps       = 0,
    .zIfCond     = 0,
    .flags       = CP_JS | F_ESM | F_BUNDLER_FRIENDLY | F_64BIT | F_NOT_IN_ALL
  },

  /*
  ** We neither build node builds on a regular basis nor test them at
  ** all. They are fully unsupported. Also, our JS targets only
  ** browsers.
  */
  .node = {
    .zEmo        = "🍟",
    .zBaseName   = "sqlite3-node",
    .zDotWasm    = 0,
    .zCmppD      = "-Dtarget:node $(c-pp.D.bundler)",
    .zEmcc       = 0,
    .zEmccExtra  = 0,
    .zEnv        = "node"
    /* Adding ",node" to the zEnv list for the other builds causes
    ** Emscripten to generate code which confuses node: it cannot
    ** reliably determine whether the build is for a browser or for
    ** node. */,
    .zDeps       = 0,
    .zIfCond     = 0,
    .flags       = CP_ALL | F_UNSUPPORTED | F_NODEJS
  },

  /* 64-bit node. */
  .node64 = {
    .zEmo        = "🍔",
    .zBaseName   = "sqlite3-node-64bit",
    .zDotWasm    = 0,
    .zCmppD      = "-Dtarget:node $(c-pp.D.bundler)",
    .zEmcc       = 0,
    .zEmccExtra  = 0,
    .zEnv        = "node",
    .zDeps       = 0,
    .zIfCond     = 0,
    .flags       = CP_ALL | F_UNSUPPORTED | F_NODEJS | F_64BIT
  },

  /* Entirely unsupported. */
  .wasmfs = {
    .zEmo        = "💿",
    .zBaseName   = "sqlite3-wasmfs",
    .zDotWasm    = 0,
    .zCmppD      = "$(c-pp.D.bundler) -Dwasmfs",
    .zEmcc       = 0,
    .zEmccExtra  =
    "-sEXPORT_ES6 -sUSE_ES6_IMPORT_META"
    " -sUSE_CLOSURE_COMPILER=0"
    " -pthread -sWASMFS -sPTHREAD_POOL_SIZE=1"
    " -sERROR_ON_UNDEFINED_SYMBOLS=0 -sLLD_REPORT_UNDEFINED"
    " -DSQLITE_ENABLE_WASMFS"
    ,
    .zEnv        = 0,
    .zDeps       = 0,
    .zIfCond     = "ifeq (1,$(wasmfs.enable))",
    .flags       = CP_ALL | F_UNSUPPORTED | F_WASMFS | F_ESM
  }
};

/*
** Emits common vars needed by the rest of the emitted code (but not
** needed by makefile code outside of these generated pieces).
*/
static void mk_prologue(void){
  /* A 0-terminated list of makefile vars which we expect to have been
  ** set up by this point in the build process. */
  char const * aRequiredVars[] = {
    "dir.top",
    "dir.api", "dir.dout", "dir.tmp",
    "dir.fiddle", "dir.fiddle.debug",
    /*"just-testing",*/
    0
  };
  char const * zVar;
  int i;
  ps(zBanner "# Build setup sanity checks...");
  for( i = 0; (zVar = aRequiredVars[i]); ++i ){
    pf("ifeq (,$(%s))\n", zVar);
    pf("  $(error build process error: expecting make var $$(%s) to "
       "have been set up by now)\n", zVar);
    ps("endif");
  }

  ps("define label.unsupported-build\n"
     "$(emo.fire)$(emo.fire)$(emo.fire)Unsupported build:"
     " use at your own risk!\n"
     "endef");

  ps(zBanner
     /** $1 = build name */
     "b.call.wasm-strip = "
     "echo '[$(emo.b.$(1)) $(out.$(1).wasm)] $(emo.strip) wasm-strip'; "
     "$(bin.wasm-strip) $(out.$(1).wasm)\n"
  );

  pf(zBanner
     "define b.do.emcc\n"
     /* $1 = build name */
     "$(bin.emcc) -o $@ $(emcc_opt_full) $(emcc.flags) "
     "$(emcc.jsflags) -sENVIRONMENT=$(emcc.environment.$(1)) "
     " $(pre-post.$(1).flags) "
     " $(emcc.flags.$(1)) "
     " $(cflags.common) $(cflags.$(1)) "
     " $(SQLITE_OPT) "
     " $(cflags.wasm_extra_init) $(sqlite3-wasm.c.in)\n"
     "endef\n"
  );

  {
    /* b.do.wasm-opt
    **
    ** $1 = build name
    **
    ** Runs $(out.$(1).wasm) through $(bin.wasm-opt)
    */
    const char * zOptFlags =
      /*
      ** Flags for wasm-opt. It has many, many, MANY "passes" options
      ** and the ones which appear here were selected solely on the
      ** basis of trial and error.
      **
      ** All wasm file size savings/costs mentioned below are based on
      ** the vanilla build of sqlite3.wasm with -Oz (our shipping
      ** configuration). Comments like "saves nothing" may not be
      ** technically correct: "nothing" means "some neglible amount."
      **
      ** Note that performance gains/losses are _not_ taken into
      ** account here: only wasm file size.
      */
      "--enable-bulk-memory-opt " /* required */
      "--all-features "           /* required */
      "--post-emscripten "        /* Saves roughly 12kb */
      "--strip-debug "            /* We already wasm-strip, but in
                                  ** case this environment has no
                                  ** wasm-strip... */
      /*
      ** The rest are trial-and-error. See wasm-opt --help and search
      ** for "Optimization passes" to find the full list.
      **
      ** With many flags this gets unusuably slow.
      */
      /*"--converge " saves nothing for the options we're using */
      /*"--dce " saves nothing */
      /*"--directize " saves nothing */
      /*"--gsi " no: requires --closed-world flag, which does not
      ** sound like something we want. */
      /*"--gufa --gufa-cast-all --gufa-optimizing " costs roughly 2kb */
      /*"--heap-store-optimization " saves nothing */
      /*"--heap2local " saves nothing */
      //"--inlining --inlining-optimizing " costs roughly 3kb */
      "--local-cse " /* saves roughly 1kb */
      /*"--once-reduction " saves nothing */
      /*"--remove-memory-init " presumably a performance tweak */
      /*"--remove-unused-names " saves nothing */
      /*"--safe-heap "*/
      /*"--vacuum " saves nothing */
      ;
    ps(zBanner
       "# post-compilation WASM file optimization");

    /* b.do.wasm-opt $1 = build name*/
    ps("ifeq (,$(bin.wasm-opt))"); {
      ps("b.do.wasm-opt = echo '$(logtag.$(1)) wasm-opt not available'");
    }
    ps("else"); {
      ps("define b.do.wasm-opt");
      pf(
        "echo '[$(emo.b.$(1)) $(out.$(1).wasm)] $(emo.wasm-opt) $(bin.wasm-opt)';\\\n"
        "\ttmpfile=$(dir.dout.$(1))/wasm-opt-tmp.$(1).wasm; \\\n"
        "\trm -f $$tmpfile; \\\n"
        "\tif $(bin.wasm-opt) $(out.$(1).wasm) "
        "-o $$tmpfile \\\n"
        "\t\t%s; then \\\n"
        "\t\tmv $$tmpfile $(out.$(1).wasm); \\\n"
#if 0
        "\t\techo -n 'After wasm-opt: '; \\\n"
        "\t\tls -l $(1); \\\n"
#endif
        /* It's very likely that the set of wasm-opt flags varies from
        ** version to version, so we'll ignore any errors here. */
        "\telse \\\n"
        "\t\trm -f $$tmpfile; \\\n"
        "\t\techo '$(logtag.$(1)) $(emo.fire) ignoring wasm-opt failure'; \\\n"
        "\tfi\n",
        zOptFlags
      );
      ps("endef");
    }
    ps("endif");
  }

  ps("more: all");
}

/*
** WASM_CUSTOM_INSTANTIATE changes how the JS pieces load the .wasm
** file from the .js file. When set, our JS takes over that step from
** Emscripten. Both modes are functionally equivalent but
** customization gives us access to wasm module state which we don't
** otherwise have. That said, the library also does not _need_ that
** state, so we don't _need_ to customize that step.
*/
#if !defined(WASM_CUSTOM_INSTANTIATE)
#  define WASM_CUSTOM_INSTANTIATE 0
#elif (WASM_CUSTOM_INSTANTIATE+0)==0
#  undef WASM_CUSTOM_INSTANTIATE
#  define WASM_CUSTOM_INSTANTIATE 0
#endif

#if WASM_CUSTOM_INSTANTIATE
/* c-pp -D... flags for the custom instantiateWasm(). */
#define C_PP_D_CUSTOM_INSTANTIATE " -DModule.instantiateWasm "
#else
#define C_PP_D_CUSTOM_INSTANTIATE
#endif

static char const * BuildDef_jsext(const BuildDef * pB){
  return (F_ESM & pB->flags) ? ".mjs" : ".js";
}

static char const * BuildDef_basename(const BuildDef * pB){
  return pB->zBaseName ? pB->zBaseName : oBuildDefs.vanilla.zBaseName;
}

/*
** Emits makefile code for setting up values for the --pre-js=FILE,
** --post-js=FILE, and --extern-post-js=FILE emcc flags, as well as
** populating those files. This is necessary for any builds which
** embed the library's JS parts of this build (as opposed to parts
** which do not use the library-level code).
**
** pB may be NULL.
*/
static void mk_pre_post(char const *zBuildName, BuildDef const * pB){
  char const * const zBaseName = pB
    ? BuildDef_basename(pB) : 0;

  assert( zBuildName );
  pf("%s# Begin --pre/--post flags for %s\n", zBanner, zBuildName);

  ps("# --pre-js=...");
  pf("pre-js.%s.js = $(dir.tmp)/pre-js.%s.js\n",
     zBuildName, zBuildName);

  if( 0==WASM_CUSTOM_INSTANTIATE || !pB ){
    pf("$(eval $(call b.c-pp.target,"
       "%s,"
       "$(pre-js.in.js),"
       "$(pre-js.%s.js),"
       "$(c-pp.D.%s)"
       "))",
       zBuildName, zBuildName, zBuildName);
  }else{
    char const *zWasmFile = pB->zDotWasm
      ? pB->zDotWasm
      : pB->zBaseName;
    /*
    ** See BuildDef::zDotWasm for _why_ we do this. _What_ we're doing
    ** is generate $(pre-js.BUILDNAME.js) as above, but:
    **
    ** 1) Add an extra -D... flag to activate the custom
    **    Module.intantiateWasm() in the JS code.
    **
    ** 2) Amend the generated pre-js.js with the name of the WASM
    **    file which should be loaded. That tells the custom
    **    Module.instantiateWasm() to use that file instead of
    **    the default.
    */
    pf("$(pre-js.%s.js): $(pre-js.in.js) $(bin.c-pp) $(MAKEFILE_LIST)",
       zBuildName);
    if( pB->zDotWasm ){
      pf(" $(dir.dout)/%s.wasm" /* This .wasm is from some other
                                   build, so this may trigger a full
                                   build of the reference copy. */,
         pB->zDotWasm);
    }
    ps("");
    pf("\t@$(call b.c-pp.shcmd,"
       "%s,"
       "$(pre-js.in.js),"
       "$(pre-js.%s.js),"
       "$(c-pp.D.%s)" C_PP_D_CUSTOM_INSTANTIATE
       ")\n",
       zBuildName, zBuildName, zBuildName);
  }

  ps("\n# --post-js=...");
  pf("post-js.%s.js = $(dir.tmp)/post-js.%s.js\n",
     zBuildName, zBuildName);
  pf("post-js.%s.in ="
     " $(dir.api)/post-js-header.js"
     " $(sqlite3-api.%s.js)"
     " $(dir.api)/post-js-footer.js\n",
     zBuildName, zBuildName);

  pf("$(eval $(call b.c-pp.target,"
     "%s,"
     "$(post-js.%s.in),"
     "$(post-js.%s.js),"
     "$(c-pp.D.%s)"
     "))\n",
     zBuildName, zBuildName, zBuildName, zBuildName);

  pf("$(post-js.%s.js): $(post-js.%s.in) $(bin.c-pp)\n",
     zBuildName, zBuildName);

  ps("\n# --extern-post-js=...");
  pf("extern-post-js.%s.js = $(dir.tmp)/extern-post-js.%s.js\n",
     zBuildName, zBuildName);
  if( 0!=WASM_CUSTOM_INSTANTIATE && zBaseName ){
    pf("$(eval $(call b.c-pp.target,"
       "%s,"
       "$(extern-post-js.in.js),"
       "$(extern-post-js.%s.js),"
       "$(c-pp.D.%s) --@policy=error -Dsqlite3.wasm=%s.wasm"
       "))",
       zBuildName, zBuildName, zBuildName,
       zBaseName);
  }else{
    pf("$(eval $(call b.c-pp.target,"
       "%s,"
       "$(extern-post-js.in.js),"
       "$(extern-post-js.%s.js),"
       "$(c-pp.D.%s)"
       "))",
       zBuildName, zBuildName, zBuildName);
  }

  ps("\n# --pre/post misc...");
  /* Combined flags for use with emcc... */
  pf("pre-post.%s.flags = "
     "--extern-pre-js=$(sqlite3-license-version.js) "
     "--pre-js=$(pre-js.%s.js) "
     "--post-js=$(post-js.%s.js) "
     "--extern-post-js=$(extern-post-js.%s.js)\n",
     zBuildName, zBuildName, zBuildName, zBuildName);


  /* Set up deps... */
  pf("pre-post.%s.deps = "
     "$(pre-post-jses.common.deps) "
     "$(post-js.%s.js) $(extern-post-js.%s.js) "
     "$(dir.tmp)/pre-js.%s.js\n",
     zBuildName, zBuildName, zBuildName, zBuildName);
  pf("# End --pre/--post flags for %s%s", zBuildName, zBanner);
}

static void emit_compile_start(char const *zBuildName){
  pf("\t@$(call b.mkdir@);"
     " $(call b.echo,%s,$(emo.compile) building ...)\n",
     zBuildName);
}

static void emit_logtag(char const *zBuildName){
#if 1
  pf("logtag.%s ?= [$(emo.b.%s)$(if $@, $@,)]:\n",
     zBuildName, zBuildName);
#else
  pf("logtag.%s ?= [$(emo.b.%s) [%s] $@]:\n",
     zBuildName, zBuildName, zBuildName);
#endif
  pf("$(info $(logtag.%s) Setting up target b-%s)\n",
     zBuildName, zBuildName );
}

/**
   Emit rules for sqlite3-api.${zBuildName}.js.
*/
static void emit_api_js(char const *zBuildName){
  pf("sqlite3-api.%s.js = $(dir.tmp)/sqlite3-api.%s.js\n",
     zBuildName, zBuildName);
  pf("$(eval $(call b.c-pp.target,"
     "%s,"
     "$(sqlite3-api.jses),"
     "$(sqlite3-api.%s.js),"
     "$(c-pp.D.%s)"
     "))\n",
     zBuildName, zBuildName, zBuildName);
  pf("$(out.%s.js): $(sqlite3-api.%s.js)\n",
     zBuildName, zBuildName);
}

/*
** Emits makefile code for one build of the library.
*/
static void mk_lib_mode(const char *zBuildName, const BuildDef * pB){
  const char * zJsExt = BuildDef_jsext(pB);
  char const * const zBaseName = BuildDef_basename(pB);

  assert( oBuildDefs.vanilla.zEnv );
  assert( zBaseName );

  pf("%s# Begin build [%s%s]. flags=0x%02x\n", zBanner,
     pB->zEmo, zBuildName, pB->flags);
  pf("# zCmppD=%s\n# zBaseName=%s\n",
     pB->zCmppD ? pB->zCmppD : "", zBaseName);
  pf("b.names += %s\n"
     "emo.b.%s = %s\n",
     zBuildName, zBuildName, pB->zEmo);
  emit_logtag(zBuildName);

  if( pB->zIfCond ){
    pf("%s\n", pB->zIfCond );
  }

  pf("dir.dout.%s ?= $(dir.dout)/%s\n", zBuildName, zBuildName);

  pf("out.%s.base ?= $(dir.dout.%s)/%s\n",
     zBuildName, zBuildName, zBaseName);
  pf("out.%s.js ?= $(dir.dout.%s)/%s%s\n",
     zBuildName, zBuildName, zBaseName, zJsExt);
  pf("out.%s.wasm ?= $(dir.dout.%s)/%s.wasm\n",
     //"$(basename $@).wasm"
     zBuildName, zBuildName, zBaseName);

  pf("dir.dout.%s ?= $(dir.dout)/%s\n", zBuildName, zBuildName);
  pf("out.%s.base ?= $(dir.dout.%s)/%s\n",
     zBuildName, zBuildName, zBaseName);

  pf("c-pp.D.%s ?= %s\n", zBuildName, pB->zCmppD ? pB->zCmppD : "");
  if( pB->flags & F_64BIT ){
    pf("c-pp.D.%s += $(c-pp.D.64bit)\n", zBuildName);
  }

  pf("emcc.environment.%s ?= %s\n", zBuildName,
     pB->zEnv ? pB->zEnv : oBuildDefs.vanilla.zEnv);
  if( pB->zEmccExtra ){
    pf("emcc.flags.%s = %s\n", zBuildName, pB->zEmccExtra);
  }

  if( pB->zDeps ){
    pf("deps.%s += %s\n", zBuildName, pB->zDeps);
  }

  emit_api_js(zBuildName);
  mk_pre_post(zBuildName, pB);

  { /* build it... */
    pf(zBanner
       "$(out.%s.js): $(MAKEFILE_LIST) $(sqlite3-wasm.c.in)"
       " $(EXPORTED_FUNCTIONS.api) $(deps.%s)"
       " $(bin.mkwb) $(pre-post.%s.deps)"
       "\n",
       zBuildName, zBuildName, zBuildName);

    emit_compile_start(zBuildName);

    if( F_UNSUPPORTED & pB->flags ){
      pf("\t@echo '$(logtag.%s) $(label.unsupported-build)'\n",
         zBuildName);
    }

    /* emcc ... */
    {
      pf("\t$(b.cmd@)$(bin.emcc) -o $@ ");
      if( pB->zEmcc ){
        pf("%s $(emcc.flags.%s)\n",
           pB->zEmcc, zBuildName);
      }else{
        pf("$(emcc_opt_full) $(emcc.flags)"
           " $(emcc.jsflags)"
           " -sENVIRONMENT=$(emcc.environment.%s)"
           " $(pre-post.%s.flags)"
           " $(emcc.flags.%s)"
           " $(cflags.common)"
           " $(cflags.%s)"
           " $(SQLITE_OPT)"
           " $(cflags.wasm_extra_init) $(sqlite3-wasm.c.in)\n",
           zBuildName, zBuildName, zBuildName, zBuildName
        );
      }
    }

    { /* Post-compilation transformations and copying to
         $(dir.dout)... */

      /* Avoid a 3rd occurrence of the bug fixed by 65798c09a00662a3,
      ** which was (in two cases) caused by makefile refactoring and
      ** not recognized until after a release was made with the broken
      ** sqlite3-bundler-friendly.mjs (which is used by the npm
      ** subproject but is otherwise untested/unsupported): */
      pf("\t@if grep -e '^ *importScripts(' $@; "
         "then echo '$(logtag.%s) $(emo.bug)$(emo.fire): "
         "bug fixed in 65798c09a00662a3 has re-appeared'; "
         "exit 1; fi;\n", zBuildName);

      if( (F_ESM & pB->flags) || (F_NODEJS & pB->flags) ){
        pf("\t@$(call b.call.patch-export-default,1,%d,$(logtag.%s))\n",
           (F_WASMFS & pB->flags) ? 1 : 0,
           zBuildName
        );
      }

      pf("\t@chmod -x $(out.%s.wasm)\n", zBuildName
         /* althttpd will automatically try to execute wasm files
            if they have the +x bit set. Why that bit is set
            at all is a mystery. */);
      pf("\t@$(call b.call.wasm-strip,%s)\n", zBuildName);

      pf("\t@$(call b.do.wasm-opt,%s)\n", zBuildName);
      pf("\t@$(call b.strip-js-emcc-bindings,$(logtag.%s))\n", zBuildName);

      if( CP_JS & pB->flags ){
        /*
        ** $(bin.emcc) will write out $@ and will create a like-named
        ** .wasm file. The resulting .wasm and .js/.mjs files are
        ** identical across all builds which have the same pB->zEmmc
        ** and/or pB->zEmccExtra.
        **
        ** For the final deliverables we copy one or both of those
        ** js/wasm files to $(dir.dout) (the top-most build target
        ** dir). We only copy the wasm file for the "base-most" builds
        ** and recycle those for the rest of the builds. The catch is:
        ** that .wasm file name gets hard-coded into $@ so we need,
        ** for cases in which we "recycle" a .wasm file from another
        ** build, to patch the name to pB->zDotWasm when copying to
        ** $(dir.dout).
        */
        if( pB->zDotWasm ){
          pf("\t@echo '$(logtag.%s) $(emo.disk) "
             "s/\"%s.wasm\"/\"%s.wasm\"/g "
             "in $(dir.dout)/$(notdir $@)'; \\\n"
             "sed"
             " -e 's/\"%s.wasm\"/\"%s.wasm\"/g'"
             " -e \"s/'%s.wasm'/'%s.wasm'/g\""
             " $@ > $(dir.dout)/$(notdir $@);\n",
             zBuildName,
             zBaseName, pB->zDotWasm,
             zBaseName, pB->zDotWasm,
             zBaseName, pB->zDotWasm);
        }else{
          pf("\t@$(call b.cp,%s,$@,$(dir.dout))\n",
             zBuildName);
        }
      }
      if( CP_WASM & pB->flags ){
        pf("\t@$(call b.cp,%s,$(basename $@).wasm,$(dir.dout))\n",
           zBuildName
           //"\techo '[%s $(out.%s.wasm)] $(emo.disk) $(dir.dout)/$(notdir $(out.%s.wasm))'
           //pB->zEmo, zBuildName
        );
      }

    }
  }
  pf("\t@$(call b.echo,%s,$(emo.done) done!%s)\n",
     zBuildName,
     (F_UNSUPPORTED & pB->flags)
     ? " $(label.unsupported-build)"
     : "");

  pf("\n%dbit: $(out.%s.js)\n"
     "$(out.%s.wasm): $(out.%s.js)\n"
     "b-%s: $(out.%s.js) $(out.%s.wasm)\n",
     (F_64BIT & pB->flags) ? 64 : 32, zBuildName,
     zBuildName, zBuildName,
     zBuildName, zBuildName, zBuildName);

  if( CP_JS & pB->flags ){
    pf("$(dir.dout)/%s%s: $(out.%s.js)\n",
       zBaseName, zJsExt, zBuildName
    );
  }

  if( CP_WASM & pB->flags ){
    pf("$(dir.dout)/%s.wasm: $(out.%s.wasm)\n",
       zBaseName, zBuildName
    );
  }

  pf("%s: $(out.%s.js)\n",
     0==((F_UNSUPPORTED | F_NOT_IN_ALL) & pB->flags)
     ? "all" : "more", zBuildName);

  if( pB->zIfCond ){
    pf("else\n"
       "$(info $(logtag.%s) $(emo.stop) disabled by condition: %s)\n"
       "endif\n",
       zBuildName, pB->zIfCond);
  }
  pf("# End build [%s]%s", zBuildName, zBanner);
}


static void emit_gz(char const *zBuildName,
                    char const *zFileExt){
  pf("\n$(out.%s.%s).gz: $(out.%s.%s)\n"
     "\t@$(call b.echo,%s,$(emo.disk))\n"
     "\t@gzip < $< > $@\n",
     zBuildName, zFileExt,
     zBuildName, zFileExt,
     zBuildName);
}

/*
** Emits rules for the fiddle builds.
*/
static void mk_fiddle(void){
  for(int i = 0; i < 2; ++i ){
    /* 0==normal, 1==debug */
    int const isDebug = i>0;
    const char * const zBuildName = i ? "fiddle.debug" : "fiddle";

    pf(zBanner "# Begin build %s\n", zBuildName);
    if( isDebug ){
      pf("emo.b.%s = $(emo.b.fiddle)$(emo.bug)\n", zBuildName);
    }else{
      pf("emo.b.fiddle = 🎻\n");
    }
    emit_logtag(zBuildName);

    pf("dir.%s = %s\n"
       "out.%s.js = $(dir.%s)/fiddle-module.js\n"
       "out.%s.wasm = $(dir.%s)/fiddle-module.wasm\n"
       "$(out.%s.wasm): $(out.%s.js)\n",
       zBuildName, zBuildName,
       zBuildName, zBuildName,
       zBuildName, zBuildName,
       zBuildName, zBuildName);

    emit_api_js(zBuildName);
    mk_pre_post(zBuildName, 0);

    {/* emcc */
      pf("$(out.%s.js): $(MAKEFILE_LIST) "
         "$(EXPORTED_FUNCTIONS.fiddle) "
         "$(fiddle.c.in) "
         "$(pre-post.%s.deps)\n",
         zBuildName, zBuildName);
      emit_compile_start(zBuildName);
      pf("\t$(b.cmd@)$(bin.emcc) -o $@"
         " $(emcc.flags.%s)" /* set in fiddle.make */
         " $(pre-post.%s.flags)"
         " $(fiddle.c.in)"
         "\n",
         zBuildName, zBuildName);
      pf("\t@chmod -x $(out.%s.wasm)\n", zBuildName);
      pf("\t@$(call b.call.wasm-strip,%s)\n", zBuildName);
      pf("\t@$(call b.strip-js-emcc-bindings,$(logtag.%s))\n",
         zBuildName);
      pf("\t@$(call b.cp,"
         "%s,"
         "$(dir.api)/sqlite3-opfs-async-proxy.js,"
         "$(dir $@))\n", zBuildName);
      if( isDebug ){
        pf("\t@$(call b.cp,%s,"
           "$(dir.fiddle)/index.html "
           "$(dir.fiddle)/fiddle.js "
           "$(dir.fiddle)/fiddle-worker.js,"
           "$(dir $@)"
           ")\n",
           zBuildName);
      }
      pf("\t@$(call b.echo,%s,$(emo.done) done!)\n", zBuildName);
    }

    pf("\n%s: $(out.%s.wasm)\n", isDebug ? "more" : "all", zBuildName);

    /* Compress fiddle files. We handle each file separately, rather
       than compressing them in a loop in the previous target, to help
       avoid that hand-edited files, like fiddle-worker.js, do not end
       up with stale .gz files (which althttpd will then serve instead
       of the up-to-date uncompressed one). */
    emit_gz(zBuildName, "js");
    emit_gz(zBuildName, "wasm");

    pf("\n%s: $(out.%s.js).gz $(out.%s.wasm).gz\n"
       "b-%s: %s\n",
       zBuildName, zBuildName, zBuildName,
       zBuildName, zBuildName);
    if( isDebug ){
      ps("fiddle-debug: fiddle.debug"); /* older name */
    }else{
      ps("all: b-fiddle");
    }
    pf("# End %s" zBanner, zBuildName);
  }
}

int main(int argc, char const ** argv){
  int rc = 0;
  const BuildDef *pB;
  pf("# What follows was GENERATED by %s. Edit at your own risk.\n", __FILE__);

  if(argc>1){
    /*
    ** Only emit the rules for the given list of builds, sans prologue
    ** (unless the arg "prologue" is given). Intended only for
    ** debugging, not actual makefile generation.
    */
    for( int i = 1; i < argc; ++i ){
      char const * const zArg = argv[i];
#define E(N) if(0==strcmp(#N, zArg)) {mk_lib_mode(# N, &oBuildDefs.N);} else /**/
      BuildDefs_map(E) if( 0==strcmp("prologue",zArg) ){
        mk_prologue();
      }else {
        fprintf(stderr,"Unkown build name: %s\n", zArg);
        rc = 1;
        break;
      }
#undef E
    }
  }else{
    /*
    ** Emit the whole shebang...
    */
    mk_prologue();
#define E(N) mk_lib_mode(# N, &oBuildDefs.N);
    BuildDefs_map(E)
#undef E
    mk_fiddle();
  }
  return rc;
}
