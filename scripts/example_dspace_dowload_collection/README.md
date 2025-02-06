# Usage Guide for download_collection.py

## Description
The `download_collection.py` script allows users to authenticate with the DSpace repository of UASLP, select a collection, retrieve its items, and download associated files (bitstreams) automatically.

## Execution
To run the script, use the following command:

```bash
python download_collection.py --email <your_email> --password <your_password> --output <output_directory>
```

## Parameters
- `--email`: Email for authentication.
- `--password`: Password for authentication.
- `--output`: Directory where the downloaded files will be stored.

## Notes
- Ensure that the provided credentials have access to the required collections.
- The script will prompt you to select a collection from the available ones.
- Files will be saved in the specified output directory.



