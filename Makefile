.PHONY: help install lint test up down dbt-run dbt-test stream clean

help:
	@echo "Available commands:"
	@echo "  make install    - Install Python dependencies"
	@echo "  make lint       - Run black, flake8, and sqlfluff"
	@echo "  make test       - Run test suite"
	@echo "  make up         - Start local Kafka and Airflow services"
	@echo "  make down       - Stop all containers"
	@echo "  make stream     - Run Kafka stream producer"
	@echo "  make dbt-run    - Run dbt models"
	@echo "  make dbt-test   - Run dbt quality tests"

install:
	pip install -r requirements.txt

lint:
	black --check src/ tests/
	flake8 src/ tests/ --max-line-length=120 --ignore=E501,W503
	cd dbt_project && sqlfluff lint models/ --dialect duckdb

test:
	pytest tests/ -v --cov=src

up:
	docker compose up -d

down:
	docker compose down -v

stream:
	python -m src.producers.order_stream_producer

consume:
	python -m src.consumers.order_stream_loader

dbt-run:
	cd dbt_project && dbt run --profiles-dir .

dbt-test:
	cd dbt_project && dbt test --profiles-dir .

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	rm -rf dbt_project/target dbt_project/dbt_packages
