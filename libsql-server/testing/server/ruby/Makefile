DB_URI ?= 'postgres://127.0.0.1:5432'

all: setup test
.PHONY: all

setup:
	bundle install
.PHONY: setup

test:
	DB_URI=$(DB_URI) bundle exec rspec postgresql_spec.rb
.PHONY: test
