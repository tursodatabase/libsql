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
** building sqlite3's WASM build. The main motivation is to generate
** code which "can" be created via GNU Make's eval command but is
** highly illegible when constructed that way. Attempts to write this
** app in Bash and TCL have suffered from the problem that both
** require escaping $ symbols, making the resulting script code as
** illegible as the eval spaghetti we want to get away from. Writing
** it in C is, somewhat surprisingly, _slightly_ less illegible than
** writing it in bash, tcl, or native Make code.
**
** The emitted makefile code is not standalone - it depends on
** variables and $(call)able functions from the main makefile.
**
*/

#undef NDEBUG
#define DEBUG 1
#include <assert.h>
#include <stdio.h>
#include <string.h>

#define pf printf
#define ps puts
/* Very common printf() args combo. */
#define zNM zName, zMode

/*
** Valid names for the zName arguments.
*/
#define JS_BUILD_NAMES sqlite3 sqlite3-wasmfs
/*
** Valid names for the zMode arguments of the "sqlite3" build. For the
** "sqlite3-wasmfs" build, only "esm" (ES6 Module) is legal.
*/
#define JS_BUILD_MODES vanilla esm bundler-friendly node
static const char * zBanner =
  "\n########################################################################\n";

/*
** Emits common vars needed by the rest of the emitted code (but not
** needed by makefile code outside of these generated pieces).
*/
static void mk_prologue(void){
  pf("%s", zBanner);
  ps("# extern-post-js* and extern-pre-js* are files for use with");
  ps("# Emscripten's --extern-pre-js and --extern-post-js flags.");
  ps("extern-pre-js.js := $(dir.api)/extern-pre-js.js");
  ps("extern-post-js.js.in := $(dir.api)/extern-post-js.c-pp.js");
  ps("# Emscripten flags for --[extern-][pre|post]-js=... for the");
  ps("# various builds.");
  ps("pre-post-common.flags := --extern-pre-js=$(sqlite3-license-version.js)");
  ps("# pre-post-jses.deps.* = a list of dependencies for the");
  ps("# --[extern-][pre/post]-js files.");
  ps("pre-post-jses.deps.common := $(extern-pre-js.js) $(sqlite3-license-version.js)");
}

/*
** Emits makefile code for setting up values for the --pre-js=FILE,
** --post-js=FILE, and --extern-post-js=FILE emcc flags, as well as
** populating those files.
*/
static void mk_pre_post(const char *zName  /* build name */,
                        const char *zMode  /* build mode */,
                        const char *zCmppD /* optional -D flags for c-pp for the
                                           ** --pre/--post-js files. */){
  pf("%s# Begin --pre/--post flags for %s-%s\n", zBanner, zName, zMode);
  pf("c-pp.D.%s-%s := %s\n", zNM, zCmppD ? zCmppD : "");
  pf("pre-post-%s-%s.flags ?=\n", zNM);

  /* --pre-js=... */
  pf("pre-js.js.%s-%s := $(dir.tmp)/pre-js.%s-%s.js\n",
     zNM, zNM);
  pf("$(pre-js.js.%s-%s): $(MAKEFILE)\n", zNM);
#if 1
  pf("$(eval $(call C-PP.FILTER,$(pre-js.js.in),$(pre-js.js.%s-%s),"
     "$(c-pp.D.%s-%s)))\n", zNM, zNM);
#else
  /* This part is needed if/when we re-enable the custom
  ** Module.instantiateModule() impl in api/pre-js.c-pp.js. */
  pf("pre-js.js.%s-%s.intermediary := $(dir.tmp)/pre-js.%s-%s.intermediary.js\n",
     zNM, zNM);
  pf("$(eval $(call C-PP.FILTER,$(pre-js.js.in),$(pre-js.js.%s-%s.intermediary),"
     "$(c-pp.D.%s-%s) -Dcustom-Module.instantiateModule))\n", zNM, zNM);
  pf("$(pre-js.js.%s-%s): $(pre-js.js.%s-%s.intermediary)\n", zNM, zNM);
  pf("\tcp $(pre-js.js.%s-%s.intermediary) $@\n", zNM);

  /* Amend $(pre-js.js.zName-zMode) for all targets except the plain
  ** "sqlite3" build... */
  if( 0!=strcmp("sqlite3-wasmfs", zName)
      && 0!=strcmp("sqlite3", zName) ){
    pf("\t@echo 'Module[xNameOfInstantiateWasm].uri = "
       "\"%s.wasm\";' >> $@\n", zName);
  }
#endif

  /* --post-js=... */
  pf("post-js.js.%s-%s := $(dir.tmp)/post-js.%s-%s.js\n", zNM, zNM);
  pf("$(eval $(call C-PP.FILTER,$(post-js.js.in),"
     "$(post-js.js.%s-%s),$(c-pp.D.%s-%s)))\n", zNM, zNM);

  /* --extern-post-js=... */
  pf("extern-post-js.js.%s-%s := $(dir.tmp)/extern-post-js.%s-%s.js\n", zNM, zNM);
  pf("$(eval $(call C-PP.FILTER,$(extern-post-js.js.in),$(extern-post-js.js.%s-%s),"
     "$(c-pp.D.%s-%s)))\n", zNM, zNM);

  /* Combine flags for use with emcc... */
  pf("pre-post-common.flags.%s-%s := "
     "$(pre-post-common.flags) "
     "--post-js=$(post-js.js.%s-%s) "
     "--extern-post-js=$(extern-post-js.js.%s-%s)\n", zNM, zNM, zNM);

  pf("pre-post-%s-%s.flags += $(pre-post-common.flags.%s-%s) "
     "--pre-js=$(pre-js.js.%s-%s)\n", zNM, zNM, zNM);

  /* Set up deps... */
  pf("pre-post-jses.%s-%s.deps := $(pre-post-jses.deps.common) "
     "$(post-js.js.%s-%s) $(extern-post-js.js.%s-%s)\n",
     zNM, zNM, zNM);
  pf("pre-post-%s-%s.deps := $(pre-post-jses.%s-%s.deps) $(dir.tmp)/pre-js.%s-%s.js\n",
     zNM, zNM, zNM);
  pf("# End --pre/--post flags for %s-%s%s", zName, zMode, zBanner);
}

/*
** Emits rules for the fiddle builds.
**
*/
static void mk_fiddle(){
  int i = 0;

  mk_pre_post("fiddle-module","vanilla", 0);
  for( ; i < 2; ++i ){
    const char *zTail = i ? ".debug" : "";
    const char *zDir = i ? "$(dir.fiddle-debug)" : "$(dir.fiddle)";

    pf("%s# Begin fiddle%s\n", zBanner, zTail);
    pf("fiddle-module.js%s := %s/fiddle-module.js\n", zTail, zDir);
    pf("fiddle-module.wasm%s := "
       "$(subst .js,.wasm,$(fiddle-module.js%s))\n", zTail, zTail);
    pf("$(fiddle-module.js%s):%s $(MAKEFILE) $(MAKEFILE.fiddle) "
       "$(EXPORTED_FUNCTIONS.fiddle) "
       "$(fiddle.cses) $(pre-post-fiddle-module-vanilla.deps) "
       "$(SOAP.js)\n",
       zTail, (i ? " $(fiddle-module.js)" : ""));
    if( 1==i ){/*fiddle.debug*/
      pf("	@test -d \"$(dir $@)\" || mkdir -p \"$(dir $@)\"\n");
    }
    pf("	$(emcc.bin) -o $@ $(fiddle.emcc-flags%s) "
       "$(pre-post-fiddle-module-vanilla.flags) $(fiddle.cses)\n",
       zTail);
    pf("	$(maybe-wasm-strip) $(fiddle-module.wasm%s)\n", zTail);
    pf("	@cp -p $(SOAP.js) $(dir $@)\n");
    if( 1==i ){/*fiddle.debug*/
      pf("	cp -p $(dir.fiddle)/index.html "
         "$(dir.fiddle)/fiddle.js "
         "$(dir.fiddle)/fiddle-worker.js "
         "$(dir $@)\n");
    }
    pf("	@for i in %s/*.*js %s/*.html %s/*.wasm; do \\\n"
       "		test -f $${i} || continue;             \\\n"
       "		gzip < $${i} > $${i}.gz; \\\n"
       "	done\n", zDir, zDir, zDir);
    if( 0==i ){
      ps("fiddle: $(fiddle-module.js)");
    }else{
      ps("fiddle-debug: $(fiddle-module-debug.js)");
    }
    pf("# End fiddle%s%s", zTail, zBanner);
  }
}

/*
** Emits makefile code for one build of the library, primarily defined
** by the combination of zName and zMode, each of which must be values
** from JS_BUILD_NAMES resp. JS_BUILD_MODES.
*/
static void mk_lib_mode(const char *zName     /* build name */,
                        const char *zMode     /* build mode */,
                        int bIsEsm            /* true only for ESM build */,
                        const char *zApiJsOut /* name of generated sqlite3-api.js/.mjs */,
                        const char *zJsOut    /* name of generated sqlite3.js/.mjs */,
                        const char *zCmppD    /* extra -D flags for c-pp */,
                        const char *zEmcc     /* extra flags for emcc */){
  assert( zName );
  assert( zMode );
  assert( zApiJsOut );
  assert( zJsOut );
  if( !zCmppD ) zCmppD = "";
  if( !zEmcc ) zEmcc = "";

  pf("%s# Begin build [%s-%s]\n", zBanner, zNM);
  pf("ifneq (1,$(MAKING_CLEAN))\n");
  pf("$(info Setting up build [%s-%s]: %s)\n", zNM, zJsOut);
  mk_pre_post(zNM, zCmppD);
  pf("\nemcc.flags.%s.%s ?=\n", zNM);
  if( zEmcc[0] ){
    pf("emcc.flags.%s.%s += %s\n", zNM, zEmcc);
  }
  pf("$(eval $(call C-PP.FILTER, $(sqlite3-api.js.in), %s, %s))\n",
     zApiJsOut, zCmppD);

  /* target zJsOut */
  pf("%s: %s $(MAKEFILE) $(sqlite3-wasm.cfiles) $(EXPORTED_FUNCTIONS.api) "
     "$(pre-post-%s-%s.deps)\n",
     zJsOut, zApiJsOut, zNM);
  pf("\t@echo \"Building $@ ...\"\n");
  pf("\t$(emcc.bin) -o $@ $(emcc_opt_full) $(emcc.flags) \\\n");
  pf("\t\t$(emcc.jsflags) -sENVIRONMENT=$(emcc.environment.%s) \\\n", zMode);
  pf("\t\t$(pre-post-%s-%s.flags) \\\n", zNM);
  pf("\t\t$(emcc.flags.%s) $(emcc.flags.%s.%s) \\\n", zName, zNM);
  pf("\t\t$(cflags.common) $(SQLITE_OPT) \\\n"
     "\t\t$(cflags.%s) $(cflags.%s.%s) \\\n"
     "\t\t$(cflags.wasm_extra_init) $(sqlite3-wasm.cfiles)\n", zName, zNM);
  if( bIsEsm ){
    /* TODO? Replace this CALL with the corresponding makefile code.
    ** OTOH, we also use this $(call) in the speedtest1-wasmfs build,
    ** which is not part of the rules emitted by this program. */
    pf("\t@$(call SQLITE3.xJS.ESM-EXPORT-DEFAULT,1,%d)\n",
       0==strcmp("sqlite3-wasmfs", zName) ? 1 : 0);
  }
  pf("\t@dotwasm=$(basename $@).wasm; \\\n"
     "\tchmod -x $$dotwasm; \\\n"
     "\t$(maybe-wasm-strip) $$dotwasm; \\\n");
  /*
  ** The above $(emcc.bin) call will write zJsOut and will create a
  ** like-named .wasm file. That .wasm file name gets hard-coded into
  ** zJsOut so we need to, for some cases, patch zJsOut to use the
  ** name sqlite3.wasm instead. Note that the resulting .wasm file is
  ** identical for all builds for which zEmcc is empty.
  */
  if( 0==strcmp("bundler-friendly", zMode)
      || 0==strcmp("node", zMode) ) {
    pf("\techo 'Patching $@ for %s.wasm...' \\\n", zName);
    pf("\trm -f $$dotwasm; dotwasm=; \\\n"
       "\tsed -i -e 's/%s-%s.wasm/%s.wasm/g' $@ || exit $$?; \\\n",
       zNM, zName);
  }
  pf("\tls -la $$dotwasm $@\n");
  if( 0!=strcmp("sqlite3-wasmfs", zName) ){
    /* The sqlite3-wasmfs build is optional and needs to be invoked
    ** conditionally using info we don't have here. */
    pf("all: %s\n", zJsOut);
  }
  ps("endif\n# ^^^ !$(MAKING_CLEAN)");
  pf("# End build [%s-%s]%s", zNM, zBanner);
}

int main(void){
  int rc = 0;
  pf("# What follows was GENERATED by %s. Edit at your own risk.\n", __FILE__);
  mk_prologue();
  mk_lib_mode("sqlite3", "vanilla", 0,
              "$(sqlite3-api.js)", "$(sqlite3.js)", 0, 0);
  mk_lib_mode("sqlite3", "esm", 1,
              "$(sqlite3-api.mjs)", "$(sqlite3.mjs)",
              "-Dtarget=es6-module", 0);
  mk_lib_mode("sqlite3", "bundler-friendly", 1,
              "$(sqlite3-api-bundler-friendly.mjs)", "$(sqlite3-bundler-friendly.mjs)",
              "$(c-pp.D.sqlite3-esm) -Dtarget=es6-bundler-friendly", 0);
  mk_lib_mode("sqlite3" , "node", 1,
              "$(sqlite3-api-node.mjs)", "$(sqlite3-node.mjs)",
              "$(c-pp.D.sqlite3-bundler-friendly) -Dtarget=node", 0);
  mk_lib_mode("sqlite3-wasmfs", "esm" ,1,
              "$(sqlite3-api-wasmfs.mjs)", "$(sqlite3-wasmfs.mjs)",
              "$(c-pp.D.sqlite3-bundler-friendly) -Dwasmfs",
              "-sEXPORT_ES6 -sUSE_ES6_IMPORT_META");

  mk_fiddle();
  mk_pre_post("speedtest1","vanilla", 0);
  mk_pre_post("speedtest1-wasmfs","esm", "$(c-pp.D.sqlite3-bundler-friendly) -Dwasmfs");
  return rc;
}
