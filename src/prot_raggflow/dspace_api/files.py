import os
from tqdm import tqdm

def download_file(session, file_url, output_path, file_name, total_size_in_bytes):
    file_response = session.get(file_url, stream=True)
    
    if file_response.status_code == 200:
        file_path = os.path.join(output_path, file_name)
        
        with open(file_path, 'wb') as f, tqdm(
                desc=f"Downloading {file_name}",
                total=total_size_in_bytes,
                unit='B', unit_scale=True) as bar:
            for chunk in file_response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))
        
        print(f"File downloaded successfully as {file_name}")
    else:
        print(f"Error {file_response.status_code} while downloading the file.")

def fetch_and_download_files(session, base_url, base_url_rest, items_ids, output_path):
    for item_id in items_ids:
        item_url = f"{base_url_rest}/items/{item_id}?expand=bitstreams"
        response = session.get(item_url)

        if response.status_code == 200:
            item_details = response.json()
            print("Item details fetched successfully.")

            bitstreams = item_details.get('bitstreams', [])
            if bitstreams:
                print(f"Found {len(bitstreams)} bitstreams.")
                file_url = bitstreams[0].get('retrieveLink', None)
                
                if file_url:
                    print(f"File download URL: {base_url}{file_url}")
                    file_name = bitstreams[0].get('name', 'downloaded_file')
                    total_size_in_bytes = bitstreams[0].get('sizeBytes', 0)
                    
                    download_file(session, f'{base_url}{file_url}', output_path, file_name, total_size_in_bytes)
                else:
                    print("No download URL found in the bitstream.")
            else:
                print("No bitstreams found for this item.")
        else:
            print(f"Error {response.status_code}: Couldn't fetch item details.")
