.PHONY: help install test pytest lint format check clean

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:  ## Install dependencies including dev dependencies
	poetry install

test: pytest  ## Run pytest tests

pytest:  ## Run pytest tests
	poetry run pytest tests/ -v

lint:  ## Run ruff linter
	poetry run ruff check .

format:  ## Format code with ruff
	poetry run ruff format .

format-check:  ## Check formatting without making changes
	poetry run ruff format --check .

type-check:  ## Run mypy type checking
	poetry run mypy focus_scrub/

check:  ## Run all checks (lint, format-check, type-check, test)
	@echo "Running lint..."
	@poetry run ruff check .
	@echo "\nChecking formatting..."
	@poetry run ruff format --check .
	@echo "\nRunning type check..."
	@poetry run mypy focus_scrub/
	@echo "\nRunning tests..."
	@poetry run pytest tests/ -v

pre-commit-install:  ## Install pre-commit hooks
	poetry run pre-commit install

pre-commit-run:  ## Run pre-commit on all files
	poetry run pre-commit run --all-files

clean:  ## Clean up cache files
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
