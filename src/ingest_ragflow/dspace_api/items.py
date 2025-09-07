import time
from typing import List, Optional

import pandas as pd
import requests
from tqdm import tqdm


def get_items(
    base_url_rest: str,
    limit_items_page: int = 100,
    max_retries: int = 3,
    verbose: bool = False,
    limit_items: Optional[int] = None,
) -> Optional[List[dict]]:
    """
    Retrieve items from DSpace.

    Args:
        base_url_rest: Base URL for DSpace REST API.
        limit_items_page: Number of items per page.
        max_retries: Maximum number of retries for failed requests.
        verbose: Wheter to print detailed information.
        limit_items: Total number of retrieve items.

    Returns:
        List of dictionaries containing metadata on the items, otherwise none.
    """

    items_url = f"{base_url_rest}/items"
    items = []
    offset = 0

    if verbose:
        print("Getting items...")
        print(f"Using limit: {limit_items_page} items per page")

    while True:
        # limit number of retriveal
        current_limit = (
            min(limit_items - len(items), limit_items_page)
            if limit_items is not None
            else limit_items_page
        )

        if limit_items is not None and len(items) >= limit_items:
            break
        params = {"limit": current_limit, "offset": offset}

        # Retry mechanism
        for attempt in range(max_retries):
            try:
                response = requests.get(items_url, params=params)
                break
            except Exception as e:
                if verbose:
                    print(f"Attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    if verbose:
                        print("Max retries reached. Stopping.")
                    return None
                time.sleep(2**attempt)  # Exponential backoff

        if response.status_code == 200:
            items_retrieved = response.json()

            if len(items_retrieved) > 0:
                items.extend(items_retrieved)

                if verbose:
                    print(
                        f"Retrieved {len(items_retrieved)} items from offset {offset} (total: {len(items)})"
                    )
                offset += len(items_retrieved)
            else:
                if verbose:
                    print("No more items found. Finishing...")
                break
        else:
            print(f"Error {response.status_code}: Items could not be obtained.")
            return None

    if verbose:
        print(f"Number of items to return: {len(items)}")

    return items


def get_items_ids(items: List[dict]) -> list[str]:
    """
    Return item IDs

    Args:
        - items: List of dictionaries containing all the metadata about the items.
    Returns:
        - List of item IDs.
    """
    items_ids = []
    for item in items:
        items_ids.append(item.get("uuid"))

    return items_ids


def get_item_stats(base_url_rest: str, item: dict) -> tuple[str, str, str, int]:
    """
    Calculate stats for a single item.

    Args:
        base_url_rest: Base URL for DSpace REST API.
        item: dictionary that containing metadata about one item.

    Returns:
        - tuple[str, str, int]: a tuple with uuid, name, name file with extension and size of the file in Bytes.
    """

    item_id = str(item.get("uuid"))
    name = str(item.get("name"))
    name_file = ""
    size_Bytes = 0

    item_url = f"{base_url_rest}/items/{item_id}?expand=bitstreams"
    response = requests.get(item_url)

    if response.status_code == 200:
        item_details = response.json()
        bitstreams = item_details.get("bitstreams", [])
        if bitstreams:
            size_Bytes = bitstreams[0].get("sizeBytes", 0)
            name_file = bitstreams[0].get("name", "")
    return item_id, name, name_file, size_Bytes


def generate_item_stats(base_url_rest: str, verbose=True) -> pd.DataFrame:
    """
    Generate statistics for all items in DSpace

    Args:
        base_url_rest: Base URL for DSpace Rest API.

    Returns:
        pd.DataFrame: DataFrame with item statistics, including
        summary row  with document counts and total size.
    """
    items = get_items(base_url_rest, verbose=verbose)
    data = []
    total_documents = 0
    total_size_all_items = 0

    for item in tqdm(items, desc="Processing items"):
        item_id, name, name_file, size_Bytes = get_item_stats(base_url_rest, item)

        data.append(
            {
                "uuid": item_id,
                "name": name,
                "name_file": name_file,
                "size_Bytes": size_Bytes,
            }
        )

        total_documents += 1
        total_size_all_items += size_Bytes

    df = pd.DataFrame(data)

    total_row = pd.DataFrame(
        {
            "uuid": ["Total documents"],
            "name": total_documents,
            "name_file": ["Total size Bytes"],
            "size_Bytes": [total_size_all_items],
        }
    )

    df = pd.concat([df, total_row], ignore_index=True)

    return df
