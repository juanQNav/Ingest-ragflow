def get_items_from_collection(session, collection_id, base_url_rest):
    items_url = f"{base_url_rest}/collections/{collection_id}/items"
    print(f"Getting items from collection {collection_id}...")
    response = session.get(items_url)
    
    if response.status_code == 200:
        items = response.json()
        print(f"The following were found {len(items)} items.\n")
        items_ids = []
        if items:
            for i, item in enumerate(items):
                item_id = item.get('uuid', 'ID not found')
                item_name = item.get('name', 'No title')
                # print(f"Index: {i} | ID: {item_id} | Title: {item_name}")
                items_ids.append(item_id)
        else:
            print("No items were found.")
        return items_ids
    else:
        print(f"Error {response.status_code}: Items could not be obtained.")
        exit()

def get_collections(session, base_url_rest):
    collections_url = f"{base_url_rest}/collections"
    print(f"Getting collections from {collections_url}...")
    response = session.get(collections_url)
    
    if response.status_code == 200:
        collections = response.json()
        print(f"The following were found {len(collections)} collections.\n")
        collections_ids = []
        if collections:
            for i, col in enumerate(collections):
                col_id = col.get('uuid', 'ID not found')
                col_name = col.get('name', 'No name')
                print(f"Index: {i} | ID: {col_id} | Name: {col_name}")
                collections_ids.append(col_id)
        else:
            print("No collections were found.")
        return collections_ids
    else:
        print(f"Error {response.status_code}: Collections could not be obtained.")
        exit()

def select_collection(collections_ids):
    while True:
        try:
            index = int(input("Enter the index of the collection you want to use: "))
            if 0 <= index < len(collections_ids):
                return collections_ids[index]
            else:
                print("Invalid index. Please enter a number within the range.")
        except ValueError:
            print("Please enter a valid number.")
