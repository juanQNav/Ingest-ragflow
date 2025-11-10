import os
from concurrent.futures import ThreadPoolExecutor

from ragflow_sdk import DataSet, Document


def read_binary_file(file_path: str) -> bytes:
    """
    Read a file in binary mode.

    Args:
        file_path: Path to the file.

    Returns:
        File content as bytes.
    """
    with open(file_path, "rb") as f:
        return f.read()


def find_pdf_files(path: str) -> list[str]:
    """
    Find all PDF files in a directory.

    Args:
        path: Path to the directory.

    Returns:
        List of absolute paths to PDF files.
    """
    return [
        os.path.join(path, f) for f in os.listdir(path) if f.endswith(".pdf")
    ]


def generate_document_list(files_paths: list[str]) -> list[dict[str, object]]:
    """
    Generete a list of document dictionaries for upload.

    Args:
        files_paths: List of file paths.

    Returns:
        List of dictionaries with:
            - displayed_name: File name.
            - blob: File content as bytes.
    """
    return [
        {
            "displayed_name": os.path.basename(file_path),
            "blob": read_binary_file(file_path),
        }
        for file_path in files_paths
    ]


def process_files_in_parallel(pdf_files: list[str]) -> list[dict[str, object]]:
    """
    Read PDF files in parallel and prepare them as document dictionaries.

    Args:
        pdf_files: List of PDF files paths.

    Returns:
        List of dictionaries with:
            - displayed_name: File name.
            - blob: File content as bytes.
    """
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(read_binary_file, pdf_files))
        return [
            {"displayed_name": os.path.basename(pdf), "blob": blob}
            for pdf, blob in zip(pdf_files, results)
        ]


def rename_document_name(document: Document, name: str) -> bool:
    """
    Rename document name.

    Args:
        document: RAGFlow document object.
        name: New name for the document (without extension).
    Returns:
        boolean, True if rename document is success,
        otherwise False.
    """
    original_name = document.name
    extension = os.path.splitext(original_name)[1]
    new_name = f"{name}{extension}"

    try:
        document.update({"name": new_name})
        return document.name == new_name
    except Exception as e:
        print(f"Error renaming the document '{new_name}': {e}")
        return False


def build_ragflow_id_docname_map(dataset: DataSet) -> dict:
    """
    Generate a mapping of RAGFlow document IDs to their PDF names.

    Args:
        dataset: RAGFlow dataset object

    Returns:
        dict: Mapping of {document_id: document_name}
    """
    document_id_to_name = {}

    if dataset is None:
        return document_id_to_name

    for document in dataset.list_documents():
        document_id_to_name[document.id] = document.name

    return document_id_to_name


def get_docs_names(dataset: DataSet) -> list[str]:
    """
    Extract all document names from a RAGFlow dataset.

    This function retrieves the names of all documents currently stored in the
    provided RAGFlow dataset. It's commonly used to check which documents exist
    before uploading new ones or when displaying available documents.

    Args:
        dataset: RAGFlow DataSet object containing documents to list

    Returns:
        list[str]: A list of document names (typically PDF filenames)
        from the dataset.
        Returns an empty list if the dataset contains no documents.
    """
    documents_names = []
    for document in dataset.list_documents():
        documents_names.append(document.name)

    return documents_names


def remove_temp_pdf(folder_path: str, processed_file_names: list[str]) -> bool:
    """
    Remove temporal pdf files after the parser has processed it.

    Args:
        folder_path: Directory path where the file will be saved.
        processed_file_names: file names list of the files with DONE
            status in RAGFlow.

    Returns:
        bool: True if the folder exists and the removal process was
            attempted (regardless of individual file success/failure),
            False if the folder path does not exist or is not a directory.
    """
    if os.path.isdir(folder_path):
        for file in processed_file_names:
            file_path_complete = os.path.join(folder_path, file)
            if os.path.exists(file_path_complete):
                try:
                    os.remove(file_path_complete)
                    print(f"File {file_path_complete} has been removed.")
                except Exception as e:
                    print(f"Error removing file {file_path_complete}: {e}")
            else:
                print(
                    f"File {file_path_complete} does not exists"
                    "(likely from previous execution), skipping..."
                )

        return True
    else:
        return False
