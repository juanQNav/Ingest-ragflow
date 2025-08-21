# RAG Module

This module provides functionalities for file manipulation and processing
within the `ingest_ragflow` project. It includes tools for reading, searching,
and parallel processing of files, as well as functions related to process
monitoring.

## Module Structure

The `rag` module contains the following files:

```md
rag/
├── _init_.py
├── README.md
├── _init_.py
├── files.py
├── parsing.py
```

## Installation

Make sure you have `tqdm` and `ragflow-sdk`

```bash
pip install requests tqdm
pip install ragflow-sdk
```

## Usage

### 1. File Manipulation (`files.py`)

- **Read a binary file:**

  ```python
  from ingest_ragflow.rag.files import read_binary_file

  content = read_binary_file("file.pdf")
  ```

- **Find PDF files in a directory:**

  ```python
  from ingest_ragflow.rag.files import find_pdf_files

  pdfs = find_pdf_files("./documents")
  print(pdfs)
  ```

- **Process files in parallel:**

  ```python
  from ingest_ragflow.rag.files import process_files_in_parallel

  results = process_files_in_parallel(pdfs)
  ```

### 2. Process Monitoring (`parsing.py`)

- **Monitor document processing:**

  ```python
  import asyncio
  from ingest_ragflow.rag.parsing import monitor_parsing

  asyncio.run(monitor_parsing(dataset, document_ids, progress_bar))
  ```
