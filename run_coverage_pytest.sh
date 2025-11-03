#!/usr/bin/env bash

source .venv/bin/activate
uv run coverage run -m pytest -v
uv run coverage report -m
