.PHONY: build clean test shell stop up

help:
	@echo "Welcome to the Python Mozaggregator\n"
	@echo "The list of commands for local development:\n"
	@echo "  build      Builds the docker images for the docker-compose setup"
	@echo "  clean      Stops and removes all docker containers"
	@echo "  shell      Opens a Bash shell"
	@echo "  stop       Stops the docker containers"
	@echo "  test       Runs the Python test suite"
	@echo "  up         Runs the whole stack, served at http://localhost:5000/"

build:
	docker-compose build

clean: stop
	docker-compose rm -f

shell: 
	docker-compose run --service-ports web bash

stop:
	docker-compose stop

test:
	docker-compose run web test

up:
	docker-compose up
