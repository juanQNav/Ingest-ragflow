# Module dspace_api

This module provides a Python interface to interact with the DSpace REST API
(UASLP), allowing authentication, collection retrieval, item retrieval, and
file downloads.

## Module Structure

```md
dspace_api/
├── **init**.py
├── README.md
├── **init**.py
├── autentification.py
├── collections.py
├── files.py
```

### Main Files

- `autentification.py`: Handles user authentication in DSpace.
- `collections.py`: Retrieves collections and items within DSpace.
- `files.py`: Downloads files (bitstreams) from retrieved items.

## Installation

Make sure you have `requests` and `tqdm` installed in your Python environment.

```bash
pip install requests tqdm
```

## Usage

### Authentication

```python
from ingest_ragflow.dspace_api.autentification import authenticate_user
import requests

session = requests.Session()
base_url_rest = "https://repositorioinstitucional.uaslp.mx/rest/"
email = "user@example.com"
password = "your_password"

authenticate_user(session, email, password, base_url_rest)
```

### Retrieve Collections

```python
from ingest_ragflow.dspace_api.collections import get_collections

collections = get_collections(base_url_rest)
print("Available collections:", collections)
```

### Retrieve Items from a Collection

```python
from ingest_ragflow.dspace_api.collections import get_items_from_collection

collection_id = "COLLECTION_UUID"
items = get_items_from_collection(collection_id, base_url_rest)
print("Items in the collection:", items)
```

### Download Files from Items

```python
from ingest_ragflow.dspace_api.files import fetch_and_download_files

output_path = "./downloads"
fetch_and_download_files("https://your-dspace.edu", base_url_rest,
items, output_path)
```

## DSpace API Endpoints Used

This module interacts with the following DSpace endpoints:

- `POST /login` - User authentication.
- `GET /collections` - Retrieves all collections.
- `GET /collections/{collectionId}/items` - Retrieves items from a collection.
- `GET /items/{itemId}?expand=bitstreams` - Retrieves item details and files.
- `GET /bitstreams/{bitstreamId}/retrieve` - Downloads files from an item.
