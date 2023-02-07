DB_URI ?= 'postgres://127.0.0.1:5432'

all: setup test
.PHONY: all

setup:
	bundle install
.PHONY: setup

test:
	LD_PRELOAD=../../../target/debug/libsqlc.so DB_URI=$(DB_URI) bundle exec rspec sqlite_spec.rb
.PHONY: test
