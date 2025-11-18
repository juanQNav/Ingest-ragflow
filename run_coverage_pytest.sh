#!/usr/bin/env bash

source .venv/bin/activate
echo -e "---------RUNING COVERAGE WITH PYTEST---------\n"
uv run coverage run -m pytest -v
echo -e "---------RUNING COVERAGE REPORT------------- \n"
uv run coverage report -m
echo -e "---------RUNING RUFF CHECK FORMAT----------- \n"
uv run ruff check
echo -e "---------RUNING FLAKE8---------------------- \n"
uv run flake8
echo -e "---------FINISH TESTS AND LINTING------------\n"
