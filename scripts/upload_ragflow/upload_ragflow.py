"""
This script uploads all the PDF files from a folder to RAGFlow and parses
them automatically.
"""

import argparse
import asyncio

from ragflow_sdk import RAGFlow
from tqdm import tqdm

from ingest_ragflow.rag.files import find_pdf_files, process_files_in_parallel
from ingest_ragflow.rag.parsing import monitor_parsing

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-folder_path", required=True, help="Folder path")

    args = vars(ap.parse_args())

    PDF_FOLDER_PATH = args["folder_path"]
    API_KEY = (
        "ragflow-I5ZDJlNjI4ZTMwYzExZWZhYzA1MDI0Mm"  # Replace with your API key
    )
    BASE_URL = "http://localhost:9380"  # Replace with your RAGFlow URL

    rag_object = RAGFlow(api_key=API_KEY, base_url=BASE_URL)

    # Ask user to create a new dataset or select an existing one
    create_new_dataset = (
        input("Create new dataset? (y/n): ").strip().lower() == "y"
    )

    if create_new_dataset:
        dataset = rag_object.create_dataset(name="test_knowledge")
        dataset.update(
            {
                "embedding_model": "mxbai-embed-large:latest",
                "chunk_method": "naive",
                "parser_config": {
                    "chunk_token_count": 128,
                    "delimiter": "\n!?;。；！？",
                    "layout_recognize": True,
                    "html4excel": False,
                    "raptor": {"use_raptor": False},
                },
            }
        )
    else:
        datasets = rag_object.list_datasets()
        for i, dataset in enumerate(datasets):
            print(f"{i}: {dataset.name}")
        selected_index = int(input("Enter dataset ID: "))
        dataset = rag_object.list_datasets(id=datasets[selected_index].id)[0]

    pdf_files = find_pdf_files(PDF_FOLDER_PATH)
    print(f"Found {len(pdf_files)} PDF files in {PDF_FOLDER_PATH}")

    documents = process_files_in_parallel(pdf_files)

    # Upload and parse documents
    with tqdm(total=len(pdf_files), desc="Processing PDFs") as pbar:
        document_ids = []
        for document in documents:
            dataset.upload_documents([document])
            document_id = dataset.list_documents()[0].id
            document_ids.append(document_id)
            dataset.async_parse_documents([document_id])

        # Monitor parsing process
        asyncio.run(monitor_parsing(dataset, document_ids))

    # Status of documents
    documents = dataset.list_documents()
    for doc in documents:
        print(
            f"Document: {doc.name}, "
            f"State: {doc.run}, "
            f"Fragments: {doc.chunk_count}"
        )
