import pandas as pd
from tqdm import tqdm


def get_items_from_collection(session, collection_id,
                              base_url_rest, verbose=False):
    items_url = f"{base_url_rest}/collections/{collection_id}/items"
    if verbose:
        print(f"Getting items from collection {collection_id}...")
    response = session.get(items_url)

    if response.status_code == 200:
        items = response.json()
        if verbose:
            print(f"The following were found {len(items)} items.\n")
        items_ids = []
        if items:
            for i, item in enumerate(items):
                item_id = item.get('uuid', 'ID not found')
                item_name = item.get('name', 'No title')
                if verbose:
                    print(f"Index: {i} | ID: {item_id} | Title: {item_name}")
                items_ids.append(item_id)
        elif verbose:
            print("No items were found.")
        return items_ids
    elif verbose:
        print(f"Error {response.status_code}: Items could not be obtained.")
        exit()


def get_collections(session, base_url_rest, verbose=False):
    collections_url = f"{base_url_rest}/collections"
    if verbose:
        print(f"Getting collections from {collections_url}...")
    response = session.get(collections_url)

    if response.status_code == 200:
        collections = response.json()
        if verbose:
            print(f"The following were found \
                   {len(collections)} collections.\n")
        collections_ids = []
        if collections:
            for i, col in enumerate(collections):
                col_id = col.get('uuid', 'ID not found')
                col_name = col.get('name', 'No name')
                if verbose:
                    print(f"Index: {i} | ID: {col_id} | Name: {col_name}")
                collections_ids.append(col_id)
        elif verbose:
            print("No collections were found.")
        return collections_ids
    elif verbose:
        print(f"Error {response.status_code}: Collections \
              could not be obtained.")
        exit()


def select_collection(collections_ids):
    while True:
        try:
            index = int(input("Enter the index of the collection \
                               you want to use: "))
            if 0 <= index < len(collections_ids):
                return collections_ids[index]
            else:
                print("Invalid index. Please enter a number within the range.")
        except ValueError:
            print("Please enter a valid number.")


def get_collection_stats(session, base_url_rest, collection_id):
    items_ids = get_items_from_collection(session,
                                          collection_id,
                                          base_url_rest) or []
    total_size = 0
    item_count = len(items_ids)

    for item_id in items_ids:
        item_url = f"{base_url_rest}/items/{item_id}?expand=bitstreams"
        response = session.get(item_url)

        if response.status_code == 200:
            item_details = response.json()
            bitstreams = item_details.get('bitstreams', [])
            if bitstreams:
                total_size += bitstreams[0].get('sizeBytes', 0)

    return item_count, total_size


def generate_collection_stats(session, base_url_rest):
    collections_ids = get_collections(session, base_url_rest)
    data = []
    total_documents = 0
    total_size_all_collections = 0

    for collection_id in tqdm(collections_ids, desc="Processing collections"):
        collection_url = f"{base_url_rest}/collections/{collection_id}"
        response = session.get(collection_url)

        if response.status_code == 200:
            collection_details = response.json()
            collection_name = collection_details.get('name', 'No name')

            item_count, total_size = get_collection_stats(session,
                                                          base_url_rest,
                                                          collection_id)
            total_documents += item_count
            total_size_all_collections += total_size

            data.append({
                'Collection Name': collection_name,
                'Collection ID': collection_id,
                'Number of Documents': item_count,
                'Total Size (Bytes)': total_size
            })

    df = pd.DataFrame(data)

    totals_row = pd.DataFrame({
        'Collection Name': ['Total'],
        'Collection ID': [''],
        'Number of Documents': [total_documents],
        'Total Size (Bytes)': [total_size_all_collections]
    })
    df = pd.concat([df, totals_row], ignore_index=True)

    return df
