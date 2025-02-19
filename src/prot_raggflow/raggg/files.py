import os
from concurrent.futures import ThreadPoolExecutor

def read_binary_file(file_path):
    with open(file_path, 'rb') as f:
        return f.read()

def find_pdf_files(path):
    return [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.pdf')]

def generate_document_list(files_paths):
    return [{"displayed_name": os.path.basename(file_path), "blob": read_binary_file(file_path)} for file_path in files_paths]

def process_files_in_parallel(pdf_files):
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(read_binary_file, pdf_files))
        return [{"displayed_name": os.path.basename(pdf), "blob": blob} for pdf, blob in zip(pdf_files, results)]
