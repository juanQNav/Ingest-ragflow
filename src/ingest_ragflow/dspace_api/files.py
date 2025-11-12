import os
from typing import List, Optional

import requests
from tqdm import tqdm

from ingest_ragflow.dspace_api.items import get_item_details


def download_file(
    file_url: str,
    output_path: str,
    file_name: str,
    total_size_in_bytes: int,
    position: int = 0,
    proxies: Optional[dict] = None,
) -> None:
    """
    Download a file from DSpace and save it locally.

    Args:
        file_url: URI of the file in DSpace.
        output_path: Directory path where the file will be saved.
        file_name: Name to save the file as.
        total_size_in_bytes: Size of the file in Bytes.
        position: Position of the progress bar in tqdm output.
        proxies: Optional dict for proxy configuration (e.g. SOCKS5).
    """
    if proxies:
        file_response = requests.get(file_url, stream=True, proxies=proxies)
    else:
        file_response = requests.get(file_url, stream=True)

    if file_response.status_code == 200:
        file_path = os.path.join(output_path, file_name)

        with (
            open(file_path, "wb") as f,
            tqdm(
                desc=f"Downloading {file_name[:30]}[...].pdf",
                total=total_size_in_bytes,
                unit="B",
                unit_scale=True,
                position=position,
                leave=False,
            ) as bar,
        ):
            for chunk in file_response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))
    else:
        tqdm.write(
            f"Error {file_response.status_code} \
                   while downloading the file."
        )


def fetch_and_download_files(
    base_url: str,
    base_url_rest: str,
    items_ids: List[str],
    output_path: str,
    proxies: Optional[dict] = None,
) -> None:
    """
    Fetch item bitstreams and download their files.

    Args:
        base_url: Base URL from RI for direct file download.
        base_url_rest: Base URL for DSpace REST API.
        items_ids: List of item IDs whose files will be downloaded.
        output_path: Directory path where files will be saved.
        proxies: Optional dict for proxy configuration (e.g. SOCKS5).
    """
    for item_id in items_ids:
        item_url = f"{base_url_rest}/items/{item_id}?expand=bitstreams"
        if proxies:
            response = requests.get(item_url, proxies=proxies)
        else:
            response = requests.get(item_url)

        if response.status_code == 200:
            item_details = response.json()

            bitstreams = item_details.get("bitstreams", [])
            if bitstreams:
                print(f"Found {len(bitstreams)} bitstreams.")
                file_url = bitstreams[0].get("retrieveLink", None)

                if file_url:
                    print(f"File download URL: {base_url}{file_url}")
                    file_name = bitstreams[0].get("name", "downloaded_file")
                    total_size_in_bytes = bitstreams[0].get("sizeBytes", 0)

                    download_file(
                        f"{base_url}{file_url}",
                        output_path,
                        file_name,
                        total_size_in_bytes,
                        proxies=proxies,
                    )
                else:
                    print("No download URL found in the bitstream.")
            else:
                print("No bitstreams found for this item.")
        else:
            print(
                f"Error {response.status_code}: Couldn't \
                   fetch item details."
            )


def retrieve_item_file(
    base_url: str,
    base_url_rest: str,
    item_id: str,
    folder_path: str,
    position: int,
    proxies: Optional[dict] = None,
) -> tuple[Optional[str], Optional[dict]]:
    """
    Retrive and download a single item's first bitstreams and return metadata.

    Args:
        base_url: Base URL form RI for direct file download.
        base_url_rest: Base URL for DSpace REST API.
        item_id: ID of the item to retrieve.
        folder_path: Directory path where the file will be saved.
        position. Position of the progress bar in tqdm output.
        proxies: Optional dict for proxy configuration (e.g. SOCKS5).

    Returns:
        Tuple (local_file_path, item_metadata) if succesful,
              (None, None) otherwise.
    """
    item_details = get_item_details(base_url_rest, item_id, proxies=proxies)
    if item_details is None:
        tqdm.write(f"[WARNING] item details for item {item_id}")
        return None, None

    bitstream = item_details.get("bitstreams", [])
    if len(bitstream) < 1:
        return None, None

    primary_bitstream = bitstream[0]
    file_url = primary_bitstream.get("retrieveLink")
    if not file_url:
        tqdm.write(f"[WARNING] No retrieve link for PDF in item {item_id}")
        return None, None

    file_name = primary_bitstream.get("name", "downloaded_file.pdf")
    file_path = os.path.join(folder_path, file_name)

    # Check if file already exists
    if os.path.exists(file_path):
        tqdm.write(
            f"[INFO] File {file_name} already exists, skipping download..."
        )
    else:
        total_size_in_bytes = primary_bitstream.get("sizeBytes", 0)
        bundle_name = primary_bitstream.get("bundleName", "UNKNOWN")

        tqdm.write(
            "[INFO] Downloading primary PDF from bundle "
            f"'{bundle_name}': {file_name}"
        )

        download_file(
            f"{base_url}{file_url}",
            folder_path,
            file_name,
            total_size_in_bytes,
            position,
            proxies=proxies,
        )

    return file_path, item_details


def get_files_from_metadata(metadata_map: dict) -> list[str]:
    """
    Extract file names from the metadata map of documents
    processed in this execution.

    Args:
        metadata_map_done: Dictionary mapping
            ragflow_document_id to item metadata.

    Returns:
        List of strings with names of pdf files.
    """
    file_names = []
    for item_metadata in metadata_map.values():
        bitstreams = item_metadata.get("bitstreams", [])
        if bitstreams:
            # Get the first bitstream's name (the PDF)
            file_name = bitstreams[0].get("name")
            if file_name:
                file_names.append(file_name)

    return file_names
