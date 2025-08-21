# `upload_ragflow.py` - Upload and Process PDFs in RAGFlow

This script automates the process of uploading PDF files to a RAGFlow dataset,
parsing them, and monitoring the processing status.

## Prerequisites

Make sure you have the required dependencies installed:

```bash
pip install requests tqdm ragflow-sdk
```

## Usage

Run the script with the -folder_path argument, specifying the directory
containing PDF files:

```bash
python upload_ragflow.py --folder_path <your folder path>
```

## Notes

- Ensure that the RAGFlow server is running and accessible at the specified BASE_URL.
- The script processes documents asynchronously; processing times may vary depending
  on the document size and server load.

