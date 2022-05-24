# This GNU makefile exists primarily to simplify/speed up development
# from emacs. It is not part of the canonical build process.
default:
	make -C ../.. wasm -e emcc_opt=-O0

clean:
	make -C ../../ clean-wasm

demo_files = emscripten.css fiddle.html \
             fiddle.js fiddle-module.js \
             fiddle-module.wasm fiddle-worker.js

# demo_target is the remote destination for the fiddle app. It
# must be a [user@]HOST:/path for rsync.
# Note that the target "should probably" contain a symlink of
# index.html -> fiddle.html.
demo_target ?=
ifeq (,$(demo_target))
ifneq (,$(wildcard /home/stephan))
  demo_target = wh2:www/wh/sqlite3/.
else ifneq (,$(wildcard /home/drh))
  #demo_target = if appropriate, add that user@host:/path here
endif
endif

push-fiddle: $(demo_files)
	@if [ x = "x$(demo_target)" ]; then \
		echo "demo_target must be a [user@]HOST:/path for rsync"; \
		exit 1; \
	fi
	rsync -va $(demo_files) $(demo_target)
