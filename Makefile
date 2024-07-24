# Makefile

.PHONY: build up down logs shell init run all

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

shell:
	docker-compose exec airflow-webserver bash

init:
	docker-compose up -d postgres
	docker-compose run --rm airflow-webserver airflow db init
	docker-compose run --rm airflow-webserver airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
	docker-compose down

run:
	docker-compose up

all:
	./setup.sh