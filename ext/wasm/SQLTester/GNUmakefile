#!/this/is/make
#
# This makefile compiles SQLTester test files into something
# we can readily import into JavaScript.
all:

SHELL := $(shell which bash 2>/dev/null)
MAKEFILE := $(lastword $(MAKEFILE_LIST))
CLEAN_FILES :=
DISTCLEAN_FILES := ./--dummy-- *~

test-list.mjs := test-list.mjs
test-list.mjs.gz := $(test-list.mjs).gz
CLEAN_FILES += $(test-list.mjs)

tests.dir := $(firstword $(wildcard tests ../../jni/src/tests))
$(info test script dir=$(tests.dir))

tests.all := $(wildcard $(tests.dir)/*.test)

bin.touint8array := ./touint8array
$(bin.touint8array): $(bin.touint8array).c $(MAKEFILE)
	$(CC) -o $@ $<
CLEAN_FILES += $(bin.touint8array)

ifneq (,$(tests.all))
$(test-list.mjs): $(bin.touint8array) $(tests.all) $(MAKEFILE)
	@{\
		echo 'export default ['; \
		sep=''; \
		for f in $(sort $(tests.all)); do \
			echo -en $$sep'{"name": "'$${f##*/}'", "content":'; \
			$(bin.touint8array) < $$f; \
			echo -n '}'; \
			sep=',\n'; \
		done; \
	echo '];'; \
	} > $@
	@echo "Created $@"
$(test-list.mjs.gz): $(test-list.mjs)
	gzip -c $< > $@
CLEAN_FILES += $(test-list.mjs.gz)
all: $(test-list.mjs.gz)
else
	@echo "Cannot build $(test-list.mjs) for lack of input test files."; \
		echo "Symlink ./tests to a directory containing SQLTester-format "; \
		echo "test scripts named *.test, then try again"; \
		exit 1
endif

.PHONY: clean distclean
clean:
	-rm -f $(CLEAN_FILES)
distclean: clean
	-rm -f $(DISTCLEAN_FILES)
