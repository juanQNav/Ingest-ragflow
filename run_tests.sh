#!/usr/bin/env bash

source .venv/bin/activate
echo -e "---------RUNING COVERAGE WITH PYTEST---------\n"
uv run coverage run -m pytest -v
echo -e "\n---------RUNING COVERAGE REPORT------------- \n"
uv run coverage report -m
echo -e "\n---------RUNING RUFF CHECK FORMAT----------- \n"
uv run ruff check
echo -e "\n---------RUNING FLAKE8---------------------- \n"
output=$(uv run flake8 --color=always)
if [ -z "$output" ]; then
  echo "All it's ok!"
else
  echo "$output"
fi
echo -e "\n---------FINISH TESTS AND LINTING------------\n"
