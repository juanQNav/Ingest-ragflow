# Ingest-ragflow

[![codecov](https://codecov.io/gh/juanQNav/Ingest-Ragflow/branch/main/graph/badge.svg)](https://codecov.io/gh/juanQNav/Ingest-Ragflow)
[![CI Tests](https://github.com/juanQNav/Ingest-Ragflow/actions/workflows/ci-test.yml/badge.svg)](https://github.com/juanQNav/Ingest-Ragflow/actions/workflows/ci-test.yml)

This repository contains code for integrating and using various components,
including the DSpace API and a knowledge base of RagFlow. The structure is
organized as
follows:

```md
src/
├── ingest_ragflow/
├── dspace_api/
│ ├── **init**.py
│ ├── README.md
│ ├── authentication.py
│ ├── collections.py
│ ├── files.py
├── rag/
│ ├── **init**.py
│ ├── README.md
│ ├── files.py
│ ├── parsing.py
```

## rag

The rag module provides functions to upload PDFs to the knowledge base.

## dspace_api

The dspace_api module interacts with the DSpace REST API endpoints.

## scripts

This directory contains the scripts used for testing and experimentation.

## Documentation

- [RAG module](src/ingest_ragflow/rag/README.md)
- [DSpace API module](src/ingest_ragflow/dspace_api/README.md)
- [Scripts](scripts/)

## Installation

To use this project, install it as a package by running the following command:

```bash
pip install setuptools
pip install -e .
```

## uv package instalation (macOS and Linux)

Install uv python package adn project manager.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## uv package instalation (Windows)

```bash
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 |
iex"
```

## uv run script

Then, in the working directory, you can synchronise a project with UV and run
any script.:

```bash
uv sync
source .venv/bin/activate.bash
uv run [any script].py

```

## To add some package to dependencies

```bash
uv add [package]
uv sync
```
