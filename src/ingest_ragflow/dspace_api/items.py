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
    proxies: Optional[dict] = None,
) -> Optional[List[dict]]:
    """
    Retrieve items from DSpace.

    Args:
        base_url_rest: Base URL for DSpace REST API.
        limit_items_page: Number of items per page.
        max_retries: Maximum number of retries for failed requests.
        verbose: Wheter to print detailed information.
        limit_items: Total number of retrieve items.
        proxies: Optional dict for proxy configuration (e.g. SOCKS5).

    Returns:
        List of dictionaries containing metadata on the items, otherwise none.
    """

    items_url = f"{base_url_rest}/items"
    items = []
    seen_uuids = set()  # Track unique UUIDs to avoid duplicates
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
                if proxies:
                    response = requests.get(
                        items_url, params=params, proxies=proxies, timeout=120
                    )
                else:
                    response = requests.get(
                        items_url, params=params, timeout=120
                    )
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
                # Deduplicate items by UUID
                unique_items = []
                for item in items_retrieved:
                    uuid = item.get("uuid")
                    if uuid and uuid not in seen_uuids:
                        seen_uuids.add(uuid)
                        unique_items.append(item)
                        # Stop if we've reached the limit
                        if (
                            limit_items is not None
                            and len(items) + len(unique_items) >= limit_items
                        ):
                            break

                items.extend(unique_items)

                if verbose:
                    duplicates_found = len(items_retrieved) - len(unique_items)
                    if duplicates_found > 0:
                        print(
                            f"Found {duplicates_found} duplicate "
                            "items (same UUID), skipping..."
                        )
                    print(
                        f"Retrieved {len(items_retrieved)} items from "
                        f"offset {offset} (total: {len(items)})"
                    )
                offset += len(items_retrieved)
                # If no unique items were added,
                # we might be stuck in duplicates
                if len(unique_items) == 0:
                    if verbose:
                        print("No new unique items found. Finishing...")
                    break
            else:
                if verbose:
                    print("No more items found. Finishing...")
                break
        else:
            print(
                f"Error {response.status_code}: Items could not be obtained."
            )
            return None

    if verbose:
        print(f"Number of items to return: {len(items)}")

    return items


def get_items_ids(items: List[dict]) -> list[str]:
    """
    Return item IDs

    Args:
        - items: List of dictionaries containing
          all the metadata about the items.
    Returns:
        - List of item IDs.
    """
    items_ids = []
    seen_uuids = set()

    for item in items:
        uuid = item.get("uuid")
        if uuid and uuid not in seen_uuids:
            seen_uuids.add(uuid)
            items_ids.append(uuid)

    return items_ids


def get_item_metadata(
    base_url_rest: str, item_id: str, proxies: Optional[dict] = None
) -> Optional[dict]:
    """
    Get complete metadata for a single item from DSpace REST API.

    Args:
        base_url_rest: Base URL for DSpace REST API.
        item_id: UUID of the item.
        proxies: Optional dict for proxy configuration (e.g. SOCKS5).

    Return:
        Dictionary with item metadata or None if error.
    """
    item_url = f"{base_url_rest}/items/{item_id}/metadata"

    try:
        if proxies:
            response = requests.get(item_url, proxies=proxies, timeout=120)
        else:
            response = requests.get(item_url, timeout=120)
        if response.status_code == 200:
            raw_metadata = response.json()
            metadata = {}

            for entry in raw_metadata:
                key = entry["key"]
                value = entry["value"]
                if isinstance(value, list):
                    metadata[key] = [v for v in value]
                else:
                    metadata[key] = value
            return metadata
        else:
            print(
                f"Error {response.status_code} getting "
                f"metadata for item {item_id}"
            )
            return None
    except Exception as e:
        print(f"Exception getting metadata for item {item_id}: {e}")
        return None


def get_item_details(
    base_url_rest: str, item_id: str, proxies: Optional[dict] = None
) -> Optional[dict]:
    """
    Get complete item details including metadata and bitstreams info.

    Args:
        base_url_rest: Base URL for DSpace REST API.
        item_id:  UUID of item.
        proxies: Optional dict for proxy configuration (e.g. SOCKS5).

    Returns:
        Dictionary with complete item details.
    """
    item_url = f"{base_url_rest}/items/{item_id}?expand=bitstreams,metadata"

    try:
        if proxies:
            response = requests.get(item_url, proxies=proxies, timeout=120)
        else:
            response = requests.get(item_url, timeout=120)
        if response.status_code == 200:
            item_data = response.json()

            metadata = get_item_metadata(
                base_url_rest, item_id, proxies=proxies
            )

            item_details = {
                "uuid": item_data.get("uuid"),
                "name": item_data.get("name"),
                "handle": item_data.get("handle"),
                "inArchive": item_data.get("inArchive"),
                "discoverable": item_data.get("discoverable"),
                "withdrawn": item_data.get("withdrawn"),
                "lastModified": item_data.get("lastModified"),
                "metadata": metadata or {},
                "bitstreams": item_data.get("bitstreams", []),
            }

            return item_details
        else:
            print(
                f"Error {response.status_code} getting details "
                f"for item {item_id}"
            )
            return None
    except Exception as e:
        print(f"Exception getting details for item {item_id}: {e}")
        return None


def get_item_stats(
    base_url_rest: str, item: dict, proxies: Optional[dict] = None
) -> tuple[str, str, str, int]:
    """
    Calculate stats for a single item.

    Args:
        base_url_rest: Base URL for DSpace REST API.
        item: dictionary that containing metadata about one item.
        proxies: Optional dict for proxy configuration (e.g. SOCKS5).

    Returns:
        - tuple[str, str, int]: a tuple with uuid, name, name file
          with extension and size of the file in Bytes.
    """

    item_id = str(item.get("uuid"))
    name = str(item.get("name"))
    name_file = ""
    size_Bytes = 0

    item_url = f"{base_url_rest}/items/{item_id}?expand=bitstreams"
    if proxies:
        response = requests.get(item_url, proxies=proxies, timeout=120)
    else:
        response = requests.get(item_url, timeout=120)

    if response.status_code == 200:
        item_details = response.json()
        bitstreams = item_details.get("bitstreams", [])
        if bitstreams:
            size_Bytes = bitstreams[0].get("sizeBytes", 0)
            name_file = bitstreams[0].get("name", "")
    return item_id, name, name_file, size_Bytes


def generate_item_stats(
    base_url_rest: str, verbose=True, proxies: Optional[dict] = None
) -> pd.DataFrame:
    """
    Generate statistics for all items in DSpace

    Args:
        base_url_rest: Base URL for DSpace Rest API.
        verbose: Whether to print detailed information.
        proxies: Optional dict for proxy configuration (e.g. SOCKS5).

    Returns:
        pd.DataFrame: DataFrame with item statistics, including
        summary row  with document counts and total size.
    """
    items = get_items(base_url_rest, verbose=verbose, proxies=proxies)
    data = []
    total_documents = 0
    total_size_all_items = 0

    for item in tqdm(items, desc="Processing items"):
        item_id, name, name_file, size_Bytes = get_item_stats(
            base_url_rest, item, proxies=proxies
        )

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
