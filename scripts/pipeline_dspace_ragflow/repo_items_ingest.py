import argparse
import asyncio
import os

from ragflow_sdk import RAGFlow
from tqdm import tqdm

from ingest_ragflow.rag.parsing import (
    monitor_parsing,
    process_items_in_parallel,
)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--li",
        required=False,
        default=None,
        type=int,
        help="Total number of retrieve items",
    )
    ap.add_argument(
        "--folder_path",
        type=str,
        required=True,
        help="Folder path for downloaded files",
    )
    ap.add_argument(
        "--apikey", type=str, required=True, help="API key for RAGFlow"
    )
    ap.add_argument(
        "--ragflow_url", type=str, required=True, help="RAGFlow URL"
    )
    ap.add_argument(
        "--max_tasks",
        default=8,
        type=int,
        help="Maximum number of concurrent tasks",
    )
    ap.add_argument(
        "--poll_interval",
        default=2.5,
        type=float,
        help="Interval (in seconds) between status checks",
    )

    args = vars(ap.parse_args())

    # Configuration
    BASE_URL = "https://repositorioinstitucional.uaslp.mx/"
    BASE_URL_REST = "https://repositorioinstitucional.uaslp.mx/rest"
    LIMIT_ITEMS = args["li"]
    FOLDER_PATH = args["folder_path"]
    MAX_CONCURRENT_TASKS = args["max_tasks"]
    POLL_INTERVAL = args["poll_interval"]

    # Create output directory if it does not exist
    os.makedirs(FOLDER_PATH, exist_ok=True)

    print("Retrieving...")

    # Initialize RAGFlow
    API_KEY = args["apikey"]
    RAG_URL = args["ragflow_url"]
    rag_object = RAGFlow(api_key=API_KEY, base_url=RAG_URL)

    # Choose dataset
    datasets = rag_object.list_datasets()
    for i, dataset in enumerate(datasets):
        print(f"{i}: {dataset.name}")
    selected_index = int(input("Enter dataset ID: "))
    dataset = rag_object.list_datasets(id=datasets[selected_index].id)[0]

    # List of documents
    document_ids = []

    process_items_in_parallel(
        base_url=BASE_URL,
        base_url_rest=BASE_URL_REST,
        folder_path=FOLDER_PATH,
        ragflow_dataset=dataset,
        document_ids=document_ids,
        max_concurrent_tasks=MAX_CONCURRENT_TASKS,
        limit_items=LIMIT_ITEMS,
    )

    # Monitoring after dowloading
    tqdm.write("Starting document parsing monitoring...")
    asyncio.run(monitor_parsing(dataset, document_ids, POLL_INTERVAL))

    # Final document status
    documents = dataset.list_documents()
    print("\nFinal Summary:")
    print("-" * 50)
    for doc in documents:
        print(
            f"{doc.name} | Status: {doc.run} |\
            Fragments: {doc.chunk_count}"
        )
    print("-" * 50)
    print("Process completed successfully.")
