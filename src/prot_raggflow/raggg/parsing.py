import asyncio
import requests
import time
import os
import threading
from tqdm import tqdm
from prot_raggflow.raggg.files import generate_document_list
from prot_raggflow.dspace_api.collections import get_items_from_collection
from prot_raggflow.dspace_api.files import retrieve_item_file
from concurrent.futures import ThreadPoolExecutor

def upload_and_parse_file(file_path, ragflow_dataset, lock, document_ids):
    document = generate_document_list([file_path])  # It is a list of dictionaries
    
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
        tqdm.write(f"[PROC] Item {os.path.basename(file_path)} processed successfully.")

def process_collections_in_parallel(session, base_url, base_url_rest, collections_ids, folder_path, ragflow_dataset, document_ids, max_concurrent_tasks=5):
    semaphore = threading.Semaphore(max_concurrent_tasks)
    lock = threading.Lock()

    def process_item(item_id, position):
        with semaphore:  
            file_path = retrieve_item_file(session, base_url, base_url_rest, item_id, folder_path, position)
            if file_path and file_path.endswith('.pdf'):
                upload_and_parse_file(file_path, ragflow_dataset, lock, document_ids)

    print("Getting items from collections...")
    with ThreadPoolExecutor() as executor:
        items_ids = []
        for id_collection in collections_ids:
            items_ids.extend(get_items_from_collection(session, id_collection, base_url_rest, verbose=False)) 

        futures = [
            executor.submit(process_item, item_id, index) for index, item_id in enumerate(items_ids)
        ]

        for future in futures:
            future.result()

def get_documents_map(dataset, document_ids):
    documents_map = {}
    for doc in dataset.list_documents():
        if doc.id in document_ids:
            documents_map[doc.id] = doc.name
    return documents_map

async def monitor_parsing(dataset, document_ids):
    documents_map = get_documents_map(dataset, document_ids)
    progress_bars = {doc_id: tqdm(total=100.00, desc=f"{doc_name[:30]}[...].pdf", position=i, leave=True) 
                     for i, (doc_id, doc_name) in enumerate(documents_map.items())}

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
                    tqdm.write(f"[INFO] Retrying to parse document {doc.name} (ID: {doc.id})...")
                    dataset.async_parse_documents([doc.id])

                    document_ids.append(doc.id)
                    progress_bars[doc.id] = tqdm(total=100.00, desc=f"{doc.name[:30]}[...].pdf", position=len(progress_bars), leave=True)
                    
                    all_done = False 
                    
        if not all_done:
            await asyncio.sleep(1)
    for bar in progress_bars.values():
        bar.close()

    tqdm.write("Parsing completed for all documents.")
