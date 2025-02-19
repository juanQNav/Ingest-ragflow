import argparse
import os
import requests
import asyncio
from tqdm import tqdm
from prot_raggflow.dspace_api.autentification import authenticate_user
from prot_raggflow.dspace_api.collections import get_collections
from ragflow_sdk import RAGFlow
from prot_raggflow.raggg.parsing import monitor_parsing, process_collections_in_parallel

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", required=True, help="Email for authentication")
    ap.add_argument("--password", required=True, help="Password for authentication")
    ap.add_argument("--folder_path", required=True, help="Folder path for downloaded files")
    ap.add_argument("--apikey", required=True, help="API key for RAGFlow")
    ap.add_argument("--ragflow_url", required=True, help="RAGFlow URL")
    ap.add_argument("--max_tasks", default=8, type=int, help="Maximum number of concurrent tasks")

    args = vars(ap.parse_args())

    # Configuration
    BASE_URL = "https://repositorioinstitucional.uaslp.mx/"
    BASE_URL_REST = "https://repositorioinstitucional.uaslp.mx/rest"
    EMAIL = args["email"]
    PASSWORD = args["password"]
    FOLDER_PATH = args["folder_path"]
    MAX_CONCURRENT_TASKS = args["max_tasks"]

    # Create output directory if it does not exist
    os.makedirs(FOLDER_PATH, exist_ok=True)

    # Initialize session
    session = requests.Session()

    # Authentication
    authenticate_user(session, EMAIL, PASSWORD, BASE_URL_REST)
    print("Getting collections...")
    collections_ids = get_collections(session, BASE_URL_REST)
    print(f"Found {len(collections_ids)} collections.")

    # Initialize RAGFlow
    API_KEY = args["apikey"]
    RAG_URL = args["ragflow_url"]
    rag_object = RAGFlow(api_key=API_KEY, base_url=RAG_URL)

    # Choose or create dataset
    create_new_dataset = input("Create new dataset? (y/n): ").strip().lower() == 'y'

    if create_new_dataset:
        dataset = rag_object.create_dataset(name="test_knowledge")
        dataset.update({
            "embedding_model": "mxbai-embed-large:latest",
            "chunk_method": "naive",
            "parser_config": {
                "chunk_token_count": 128,
                "delimiter": "\n!?;。；！？",
                "layout_recognize": True,
                "html4excel": False,
                "raptor": {"use_raptor": False}
            }
        })
    else:
        datasets = rag_object.list_datasets()
        for i, dataset in enumerate(datasets):
            print(f'{i}: {dataset.name}')
        selected_index = int(input("Enter dataset ID: "))
        dataset = rag_object.list_datasets(id=datasets[selected_index].id)[0]

    # List of documents
    document_ids = []

    # Retrieve items from collections and process them in parallel
    process_collections_in_parallel(
        session=session,
        base_url=BASE_URL,
        base_url_rest=BASE_URL_REST,
        collections_ids=collections_ids,
        folder_path=FOLDER_PATH,
        ragflow_dataset=dataset,
        document_ids=document_ids,
        max_concurrent_tasks=MAX_CONCURRENT_TASKS
    )

    # Monitoring after downloading
    tqdm.write("Starting document parsing monitoring...")
    asyncio.run(monitor_parsing(dataset, document_ids))

    # Final document status
    documents = dataset.list_documents()
    print("\nFinal Summary:")
    print("-" * 50)
    for doc in documents:
        print(f"📄 {doc.name} | Status: {doc.run} | Fragments: {doc.chunk_count}")
    print("-" * 50)
    print("Process completed successfully.")
