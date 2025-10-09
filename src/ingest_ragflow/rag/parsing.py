import asyncio
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import requests
from ragflow_sdk.modules.dataset import DataSet
from tqdm import tqdm

from ingest_ragflow.dspace_api.collections import get_items_from_collection
from ingest_ragflow.dspace_api.files import retrieve_item_file
from ingest_ragflow.dspace_api.items import get_items, get_items_ids
from ingest_ragflow.rag.files import generate_document_list


def upload_and_parse_file(
    file_path: str, ragflow_dataset: DataSet, lock, document_ids: list[str]
):
    """
    Upload a file to RagFlow dataset and trigger parsing.

    Args:
        file_path: Path to the file to upload.
        ragflow_dataset: RagFlow dataset object.
        lock: Threading lock to ensure thread-safe upload/parse.
        document_ids: List to append the new document ID after upload.
    """
    document = generate_document_list([file_path])  # It is a list of dicts

    with lock:  # Ensures that only one thread at a time can upload documents
        ragflow_dataset.upload_documents(document)
        document_id = ragflow_dataset.list_documents()[0].id
        document_ids.append(document_id)

    try:
        ragflow_dataset.async_parse_documents([document_id])
    except requests.exceptions.ConnectionError as e:
        tqdm.write(f"[ERROR] Connection error: {e}")
        time.sleep(5)  # Wait 5 seconds before retrying
        ragflow_dataset.async_parse_documents([document_id])  # Retry
    except Exception as e:
        tqdm.write(f"[ERROR] Unexpected error: {e}")

    with lock:
        tqdm.write(
            f"[PROC] Item {os.path.basename(file_path)} \
                   processed successfully."
        )


def process_item(
    base_url: str,
    base_url_rest: str,
    item_id: str,
    folder_path: str,
    position: int,
    ragflow_dataset: DataSet,
    lock,
    documents_ids: list[str],
) -> tuple[Optional[str], Optional[dict]]:
    """
    Process a single item: download file and return metadata.

    Args:
        base_url: Base URL for direct file download.
        base_url_rest: Base URL for DSpace REST API.
        item_id: ID of the item to retrieve.
        folder_path: Directory where downloaded files will be stored.
        position: Position of the progress bar in tqdm output.
        ragflow_dataset: RagFlow dataset object.
        lock: Threading lock to ensure thread-safe upload/parse.
        documents_ids:

    Returns:
        Tuple (ragflow_document_id, item_metadata) if succesful, (None, None) otherwise.
    """
    file_path, item_metadata = retrieve_item_file(
        base_url=base_url,
        base_url_rest=base_url_rest,
        item_id=item_id,
        folder_path=folder_path,
        position=position,
    )

    if file_path and file_path.endswith(".pdf") and item_metadata:
        document = generate_document_list([file_path])

        with lock:
            ragflow_dataset.upload_documents(document)
            documents_id = ragflow_dataset.list_documents()[0].id
            documents_ids.append(documents_id)

        try:
            ragflow_dataset.async_parse_documents([documents_id])
        except requests.exceptions.ConnectionError as e:
            tqdm.write(f"[ERROR] Connection error: {e}")
            time.sleep(5)
            ragflow_dataset.async_parse_documents([documents_id])
        except Exception as e:
            tqdm.write(f"[Error] Unexpected error: {e}")

        with lock:
            tqdm.write(
                f"[Proc] item {os.path.basename(file_path)} \
                processed successfully."
            )

        return documents_id, item_metadata

    return None, None


def process_items_in_parallel(
    base_url: str,
    base_url_rest: str,
    folder_path: str,
    ragflow_dataset: DataSet,
    document_ids: list[str],
    max_concurrent_tasks: int = 5,
    limit_items: Optional[int] = None,
) -> dict[str, str]:
    """
    Process items in parallel:
    - Retrieve items from DSpace.
    - Download files from DSpace.
    - Upload and parse PDFs in RagFlow.
    - Return metadata mapping.

    Args:
        base_url: Base URL for direct file download.
        base_url_rest: Base URL for DSpace REST API.
        folder_path: Directory where downloaded files will be stored.
        ragflow_dataset: RagFlow dataset object.
        document_ids: List to store parsed document IDs.
        max_concurrent_tasks: Maximum number of concurrent tasks.
        limit_items: Total number of retrieve items.

    Returns:
        Dictionary mapping ragflow_document_id to item metadata.
    """
    semaphore = threading.Semaphore(max_concurrent_tasks)
    lock = threading.Lock()
    metadata_map = {}

    def process_single_item(item_id, position):
        with semaphore:
            ragflow_id, metadata = process_item(
                base_url=base_url,
                base_url_rest=base_url_rest,
                item_id=item_id,
                folder_path=folder_path,
                position=position,
                ragflow_dataset=ragflow_dataset,
                lock=lock,
                documents_ids=document_ids,
            )
            if ragflow_id and metadata:
                with lock:
                    metadata_map[ragflow_id] = metadata

    with ThreadPoolExecutor() as executor:
        items = get_items(base_url_rest, verbose=True, limit_items=limit_items)
        if items is not None:
            items_ids = get_items_ids(items)

        futures = [
            executor.submit(process_single_item, item_id, index)
            for index, item_id in enumerate(items_ids)
        ]

        for future in futures:
            future.result()

    return metadata_map


def process_collections_in_parallel(
    base_url: str,
    base_url_rest: str,
    collections_ids: list[str],
    folder_path: str,
    ragflow_dataset: DataSet,
    document_ids: list[str],
    max_concurrent_tasks: int = 5,
) -> dict[str, str]:
    """
    Process collections in parallel:
    - Retrieve items from collections.
    - Download files from DSpace.
    - Upload and parse PDFs in RagFlow.
    - Return metadata mapping.

    Args:
        base_url: Base URL for direct file download.
        base_url_rest: Base URL for DSpace REST API.
        collections_ids: List of collection IDs to process.
        folder_path: Directory where downloaded files will be stored.
        ragflow_dataset: RagFlow dataset object.
        document_ids: List to store parsed document IDs.
        max_concurrent_tasks: Maximum number of concurrent tasks.

    Returns:
        Dictionary mapping ragflow_document_id to item metadata.
    """
    semaphore = threading.Semaphore(max_concurrent_tasks)
    lock = threading.Lock()
    metadata_map = {}

    def process_single_item(item_id, position):
        with semaphore:
            ragflow_id, metadata = process_item(
                base_url=base_url,
                base_url_rest=base_url_rest,
                item_id=item_id,
                folder_path=folder_path,
                position=position,
                ragflow_dataset=ragflow_dataset,
                lock=lock,
                documents_ids=document_ids,
            )
            if ragflow_id and metadata:
                with lock:
                    metadata_map[ragflow_id] = metadata

    with ThreadPoolExecutor() as executor:
        items_ids = []
        for id_collection in collections_ids:
            items = get_items_from_collection(
                id_collection, base_url_rest, verbose=False
            )
            if items is not None:
                items_ids.extend(items)

        futures = [
            executor.submit(process_single_item, item_id, index)
            for index, item_id in enumerate(items_ids)
        ]

        for future in futures:
            future.result()

    return metadata_map


def get_documents_map(
    dataset: DataSet, document_ids: list[str]
) -> dict[str, str]:
    """
    Map document IDs to their names for a given dataset.

    Args:
        dataset: RagFlow dataset object.
        documents_ids: List of document IDs.

    Returns:
        Dictionary mapping document IDs to document names.
    """
    documents_map = {}
    for doc in dataset.list_documents():
        if doc.id in document_ids:
            documents_map[doc.id] = doc.name
    return documents_map


async def monitor_parsing(
    dataset: DataSet, document_ids: list[str], poll_interval: float = 2.5
) -> None:
    """
    Monitor the parsing progress of documents in RagFlow.

    Args:
        dataset: RagFlow dataset object.
        document_ids: List of document IDs to monitor.
        poll_interval: Interval (in seconds) between status checks.
    """
    documents_map = get_documents_map(dataset, document_ids)
    progress_bars = {
        doc_id: tqdm(
            total=100.00,
            desc=f"{doc_name[:30]}[...].pdf",
            position=i,
            leave=True,
        )
        for i, (doc_id, doc_name) in enumerate(documents_map.items())
    }

    all_done = False
    while not all_done:
        all_done = True
        documents = dataset.list_documents()

        for doc in documents:
            if doc.id in document_ids:
                progress = doc.progress * 100
                progress_bars[doc.id].n = round(progress, 2)
                progress_bars[doc.id].refresh()

                if doc.run == "RUNNING":
                    all_done = False
            else:
                if doc.run == "UNSTART":
                    tqdm.write(
                        f"[INFO] Retrying to parse document {doc.name}\
                        (ID: {doc.id})..."
                    )
                    dataset.async_parse_documents([doc.id])

                    document_ids.append(doc.id)
                    progress_bars[doc.id] = tqdm(
                        total=100.00,
                        desc=f"{doc.name[:30]}[...].pdf",
                        position=len(progress_bars),
                        leave=True,
                    )

                    all_done = False

        if not all_done:
            await asyncio.sleep(poll_interval)
    for bar in progress_bars.values():
        bar.close()

    tqdm.write("Parsing completed for all documents.")
