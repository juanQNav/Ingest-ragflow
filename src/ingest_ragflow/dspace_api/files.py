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
) -> None:
    """
    Download a file from DSpace and save it locally.

    Args:
        file_url: URI of the file in DSpace.
        output_path: Directory path where the file will be saved.
        file_name: Name to save the file as.
        total_size_in_bytes: Size of the file in Bytes.
        position: Position of the progress bar in tqdm output.
    """
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
) -> None:
    """
    Fetch item bitstreams and download their files.

    Args:
        base_url: Base URL from RI for direct file download.
        base_url_rest: Base URL for DSpace REST API.
        items_ids: List of item IDs whose files will be downloaded.
        output_path: Directory path where files will be saved.
    """
    for item_id in items_ids:
        item_url = f"{base_url_rest}/items/{item_id}?expand=bitstreams"
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


def get_primary_pdf_bitstream(bitstreams: List[dict]) -> Optional[dict]:
    """
    Get the primary PDF bitstreams from a list of bitstreams.

    Prioritizes:
    1. PDFs in ORIGINAL bundle (main document)
    2. Largest PDF file
    3. First PDF found

    Args:
        bitstreams: List of bitstreams dictionaries.

    Returns:
        The primary PDF bitstream or None if no PDF found.
    """
    if not bitstreams:
        return None

    pdf_bitstreams = [
        bs for bs in bitstreams if bs.get("name", "").lower().endswith(".pdf")
    ]

    if not pdf_bitstreams:
        return None

    # Priority 1: ORIGINAL bundle
    original_pdfs = [
        bs for bs in pdf_bitstreams if bs.get("bundleName") == "ORIGINAL"
    ]

    # If multiple PDFs in ORIGINAL, return the Largest
    if original_pdfs:
        return max(original_pdfs, key=lambda x: x.get("sizeBytes", 0))

    # # Priority 2: Return the Largest PDF
    return max(pdf_bitstreams, key=lambda x: x.get("sizeBytes", 0))


def retrieve_item_file(
    base_url: str,
    base_url_rest: str,
    item_id: str,
    folder_path: str,
    position: int,
) -> tuple[Optional[str], Optional[dict]]:
    """
    Retrive and download a single item's first bitstreams and return metadata.

    Args:
        base_url: Base URL form RI for direct file download.
        base_url_rest: Base URL for DSpace REST API.
        item_id: ID of the item to retrieve.
        folder_path: Directory path where the file will be saved.
        position. Position of the progress bar in tqdm output.

    Returns:
        Tuple (local_file_path, item_metadata) if succesful, (None, None) otherwise.
    """
    item_details = get_item_details(base_url_rest, item_id)

    if not item_details:
        tqdm.write(f"[WARNING] Could not get details for item {item_id}")
        return None, None

    bitstreams = item_details.get("bitstreams", [])

    if not bitstreams:
        tqdm.write(f"[WARNING] No bitstreams found for item {item_id}")
        return None, None

    # Get the primary PDF bitstream
    primary_bitstream = get_primary_pdf_bitstream(bitstreams)

    if not primary_bitstream:
        tqdm.write(f"[WARNING] No PDF bitstream found for item {item_id}")
        return None, None

    # Update item_details to only include the primary bitstream
    # keep it as a list for consitency
    item_details["bitstreams"] = [primary_bitstream]

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
            f"[INFO] Downloading primary PDF from bundle '{bundle_name}': {file_name}"
        )

        download_file(
            f"{base_url}{file_url}",
            folder_path,
            file_name,
            total_size_in_bytes,
            position,
        )

    return file_path, item_details
