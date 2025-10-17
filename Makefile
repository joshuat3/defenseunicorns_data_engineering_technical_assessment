.DEFAULT_GOAL := help

SHELL := /bin/bash

# The current branch name
BRANCH := $(shell git symbolic-ref --short HEAD)
# The "primary" directory
PRIMARY_DIR := $(shell pwd)


# Idiomatic way to force a target to always run, by having it depend on this dummy target
FORCE:

.PHONY: help
help: ## Show available user-facing targets
	grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	| sed -n 's/^\(.*\): \(.*\)##\(.*\)/\1:\3/p' \
	| column -t -s ":"

.PHONY: help-dev
help-dev: ## Show available dev-facing targets
	grep -E '^_[a-zA-Z0-9_-]+:.*?#_# .*$$' $(MAKEFILE_LIST) \
	| sed -n 's/^\(.*\): \(.*\)#_#\(.*\)/\1:\3/p' \
	| column -t -s ":"

.PHONY: dev-up
dev-up: ## Brings up a fresh postgresql server available on localhost port 5432
	$(MAKE) _test-postgres-down _test-postgres-up
	sleep 2
	$(MAKE) _test-schema-up

.PHONY: submit
submit: ## Gets a pg_dump of the orders database and saves to submission/pg_dump.tar.gz
	$(MAKE) _test-check-submission-dir _test-pg-dump _test-src-dump

.PHONY: ingest
ingest: ## WARNING This will recycle the postgres deployment.  Runs a full ingestion test with docker compose
	$(MAKE) _test-postgres-down _test-build-solution_test-compose-solution
	$(MAKE) _test-check-submission-dir _test-compose-logs _test-compose-down

.PHONY: run-tests
run-tests: ## Runs the tests in src/tests.py using pytest
	cd src && python -m pytest tests.py

.PHONY: _test-pg-dump
_test-pg-dump: #_# Executes a pg dump of the orders database to submissions
	docker exec postgres pg_dump -U orders -F t orders | >./submission/pg_dump.tar

.PHONY: _test-src-dump
_test-src-dump: #_# creates a tar.gz of the ./src directory
	tar -czvf submission/src.tar.gz ./src

.PHONY: _test-postgres-up
_test-postgres-up: #_# Brings up a postgres container with the correct database / user / password
	docker run \
		--name postgres \
		-p 5432:5432 \
		-e POSTGRES_USER=orders \
		-e POSTGRES_PASSWORD=s3cr3tp455w0rd \
		-e POSTGRES_DB=orders \
		-d \
		postgres:17.4

.PHONY: _test-postgres-down
_test-postgres-down: #_# Brings down the postgres container
	if [ "$(shell docker ps -q -f name=postgres)" ]; then \
		docker stop "$(shell docker ps -q -f name=postgres)"; \
		docker remove "$(shell docker ps -q -f name=postgres)"; \
	fi

.PHONY: _test-check-submission-dir
_test-check-submission-dir: #_# Creates the submission directory if it doesn't exist
	if [ -d "./submission" ]; then \
		echo "submission Directory exists"; \
	else \
		mkdir submission; \
	fi

.PHONY: _test-schema-up
_test-schema-up: #_# Creates the database schema for psql
	cat ./postgres/schema.sql | docker exec -i postgres psql -U orders -d orders

.PHONY: _test-build-solution
_test-build-solution: #_# Creates the docker image of the solution
	docker build -t solution:latest -f ./src/Dockerfile .

.PHONY: _test-compose-solution
_test-compose-solution: #_# gets the solution up and running
	docker compose up -d

.PHONY: _test-compose-down
_test-compose-down: #_# Brings down the docker compose solution
	docker compose down

.PHONY: _test-compose-logs
_test-compose-logs: #_# streams the logs of the solution container and saves them to submission directory
	docker compose logs solution -f
	docker compose logs solution -t >> solution_logs.txt
