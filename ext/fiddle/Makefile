# This makefile exists primarily to simplify/speed up development from
# emacs. It is not part of the canonical build process.
default:
	make -C ../.. wasm -e emcc_opt=-O0

clean:
	make -C ../../ clean-wasm

push-demo:
	rsync -va fiddle*.js fiddle*.wasm fiddle.html *.css wh2:www/wh/sqlite3/.
