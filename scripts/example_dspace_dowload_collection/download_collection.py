"""
This script downloads all the files from a collection in DSpace automatically.
"""

import argparse
import os

import requests

from ingest_ragflow.dspace_api.autentification import authenticate_user
from ingest_ragflow.dspace_api.collections import (
    get_collections,
    get_items_from_collection,
    select_collection,
)
from ingest_ragflow.dspace_api.files import fetch_and_download_files

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-email", required=True, help="Email for authentication")
    ap.add_argument("-password", required=True, help="Password for authentication")
    ap.add_argument("-output", required=True, help="Output path for downloaded files")

    args = vars(ap.parse_args())

    # Configuration
    BASE_URL = "https://repositorioinstitucional.uaslp.mx/"
    BASE_URL_REST = "https://repositorioinstitucional.uaslp.mx/rest"
    OUTPUT_PATH = args["output"]
    EMAIL = args["email"]
    PASSWORD = args["password"]

    # Create output directory
    os.makedirs(OUTPUT_PATH, exist_ok=True)

    # Initialize session
    session = requests.Session()

    # Authenticate user
    authenticate_user(session, EMAIL, PASSWORD, BASE_URL_REST)

    # Get collections and select one
    collections_ids = get_collections(session, BASE_URL_REST)
    if collections_ids is not None:
        collection_id = select_collection(collections_ids)

        # Get items from the selected collection
        items_ids = get_items_from_collection(session, collection_id, BASE_URL_REST)

        if items_ids is not None:
            # Fetch and download files
            fetch_and_download_files(
                session, BASE_URL, BASE_URL_REST, items_ids, OUTPUT_PATH
            )
        else:
            print("The IDs for the collections were not found.")
    else:
        print("No collections found")
