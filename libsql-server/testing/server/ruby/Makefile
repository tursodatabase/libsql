all: setup test
.PHONY: all

setup:
	bundle install
.PHONY: setup

test:
	bundle exec rspec postgresql_spec.rb
.PHONY: test
