import os
from typing import List, Optional

from requests import Session
from tqdm import tqdm


def download_file(
    session: Session,
    file_url: str,
    output_path: str,
    file_name: str,
    total_size_in_bytes: int,
    position: int = 0,
) -> None:
    """
    Download a file from DSpace and save it locally.

    Args:
        session: requests Session object.
        file_url: URI of the file in DSpace.
        output_path: Directory path where the file will be saved.
        file_name: Name to save the file as.
        total_size_in_bytes: Size of the file in Bytes.
        position: Position of the progress bar in tqdm output.
    """
    file_response = session.get(file_url, stream=True)

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
    session: Session,
    base_url: str,
    base_url_rest: str,
    items_ids: List[str],
    output_path: str,
) -> None:
    """
    Fetch item bitstreams and download their files.

    Args:
        session: requests Session object.
        base_url: Base URL from RI for direct file download.
        base_url_rest: Base URL for DSpace REST API.
        items_ids: List of item IDs whose files will be downloaded.
        output_path: Directory path where files will be saved.
    """
    for item_id in items_ids:
        item_url = f"{base_url_rest}/items/{item_id}?expand=bitstreams"
        response = session.get(item_url)

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
                        session,
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


def retrieve_item_file(
    session: Session,
    base_url: str,
    base_url_rest: str,
    item_id: str,
    folder_path: str,
    position: int,
) -> Optional[str]:
    """
    Retrive and download a single item's first bitstreams.

    Args:
        session: requests Session object.
        base_url: Base URL form RI for direct file download.
        base_url_rest: Base URL for DSpace REST API.
        item_id: ID of the item to retrieve.
        folder_path: Directory path where the file will be saved.
        position. Position of the progress bar in tqdm output.

    Returns:
        Local file path if the file was successfully downloaded,
    """
    item_url = f"{base_url_rest}/items/{item_id}?expand=bitstreams"
    response = session.get(item_url)

    if response.status_code == 200:
        item_details = response.json()
        bitstreams = item_details.get("bitstreams", [])

        if bitstreams:
            file_url = bitstreams[0].get("retrieveLink", None)
            if file_url:
                file_name = bitstreams[0].get("name", "downloaded_file")
                file_path = os.path.join(folder_path, file_name)

                if not os.path.exists(file_path):
                    total_size_in_bytes = bitstreams[0].get("sizeBytes", 0)
                    download_file(
                        session,
                        f"{base_url}{file_url}",
                        folder_path,
                        file_name,
                        total_size_in_bytes,
                        position,
                    )
                else:
                    tqdm.write(
                        f"[INFO] File {file_name} already exists, \
                               skipping download..."
                    )
                return file_path
    return None
