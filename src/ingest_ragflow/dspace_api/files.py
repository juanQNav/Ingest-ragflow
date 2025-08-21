import os
from tqdm import tqdm


def download_file(session, file_url, output_path, file_name,
                  total_size_in_bytes, position=0):
    file_response = session.get(file_url, stream=True)

    if file_response.status_code == 200:
        file_path = os.path.join(output_path, file_name)

        with open(file_path, 'wb') as f, tqdm(
                desc=f"Downloading {file_name[:30]}[...].pdf",
                total=total_size_in_bytes,
                unit='B', unit_scale=True,
                position=position,
                leave=False
        ) as bar:
            for chunk in file_response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))
    else:
        tqdm.write(f"Error {file_response.status_code} \
                   while downloading the file.")


def fetch_and_download_files(session, base_url, base_url_rest,
                             items_ids, output_path):
    for item_id in items_ids:
        item_url = f"{base_url_rest}/items/{item_id}?expand=bitstreams"
        response = session.get(item_url)

        if response.status_code == 200:
            item_details = response.json()

            bitstreams = item_details.get('bitstreams', [])
            if bitstreams:
                print(f"Found {len(bitstreams)} bitstreams.")
                file_url = bitstreams[0].get('retrieveLink', None)

                if file_url:
                    print(f"File download URL: {base_url}{file_url}")
                    file_name = bitstreams[0].get('name', 'downloaded_file')
                    total_size_in_bytes = bitstreams[0].get('sizeBytes', 0)

                    download_file(session, f'{base_url}{file_url}',
                                  output_path, file_name, total_size_in_bytes)
                else:
                    print("No download URL found in the bitstream.")
            else:
                print("No bitstreams found for this item.")
        else:
            print(f"Error {response.status_code}: Couldn't \
                   fetch item details.")


def retrieve_item_file(session, base_url, base_url_rest,
                       item_id, folder_path, position):
    item_url = f"{base_url_rest}/items/{item_id}?expand=bitstreams"
    response = session.get(item_url)

    if response.status_code == 200:
        item_details = response.json()
        bitstreams = item_details.get('bitstreams', [])

        if bitstreams:
            file_url = bitstreams[0].get('retrieveLink', None)
            if file_url:
                file_name = bitstreams[0].get('name', 'downloaded_file')
                file_path = os.path.join(folder_path, file_name)

                if not os.path.exists(file_path):
                    total_size_in_bytes = bitstreams[0].get('sizeBytes', 0)
                    download_file(
                        session, f'{base_url}{file_url}',
                        folder_path, file_name,
                        total_size_in_bytes, position)
                else:
                    tqdm.write(f"[INFO] File {file_name} already exists, \
                               skipping download...")
                return file_path
    return None
