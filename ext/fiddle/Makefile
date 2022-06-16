# This GNU makefile exists primarily to simplify/speed up development
# from emacs. It is not part of the canonical build process.
default:
	$(MAKE) -C ../.. wasm -e emcc_opt=-O0

clean:
	$(MAKE) -C ../../ clean-wasm

fiddle_files = emscripten.css fiddle.html \
             fiddle.js fiddle-module.js \
             fiddle-module.wasm fiddle-worker.js \
             $(wildcard *.wasm.gz) $(wildcard *.js.gz)

# fiddle_remote is the remote destination for the fiddle app. It
# must be a [user@]HOST:/path for rsync.
# Note that the target "should probably" contain a symlink of
# index.html -> fiddle.html.
fiddle_remote ?=
ifeq (,$(fiddle_remote))
ifneq (,$(wildcard /home/stephan))
  fiddle_remote = wh2:www/wh/sqlite3/.
else ifneq (,$(wildcard /home/drh))
  #fiddle_remote = if appropriate, add that user@host:/path here
endif
endif

$(fiddle_files): default

push-fiddle: $(fiddle_files)
	@if [ x = "x$(fiddle_remote)" ]; then \
		echo "fiddle_remote must be a [user@]HOST:/path for rsync"; \
		exit 1; \
	fi
	rsync -va $(fiddle_files) $(fiddle_remote)
