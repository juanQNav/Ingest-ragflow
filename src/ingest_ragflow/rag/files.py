import os
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

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


def get_all_documents(
    dataset: DataSet,
    keywords: Optional[str] = None,
    orderby: str = "create_time",
    desc: bool = True,
    page_size: int = 100,
    verbose: bool = False,
) -> list[Document]:
    """
    Get all documents from a dataset handling pagination automatically.

    Args:
        dataset: RAGFlow dataset object
        keywords: Keywords to filter documents by title
        order_by: Field to sort by ("create_time" or "update_time")
        desc: Sort in descending order
        page_size: Number of documents per page (default: 100)
        verbose: Print pagination progress

    Returns:
        list: All documents matching the criteria
    """
    if dataset is None:
        return []

    all_documents: list[Document] = []
    page = 1

    while True:
        try:
            documents = dataset.list_documents(
                keywords=keywords,
                page=page,
                page_size=page_size,
                orderby=orderby,
                desc=desc,
            )

            if not documents:
                break

            all_documents.extend(documents)

            if verbose:
                print(
                    f"Fetched page {page}: {len(documents)} documents "
                    f"(Total: {len(all_documents)})"
                )

            if len(documents) < page_size:
                break

            page += 1

        except Exception as e:
            if verbose:
                print(f"Error fetching page {page}: {e}")
            break

    if verbose:
        print(f"Total documents retrieved: {len(all_documents)}")

    return all_documents


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


def get_orphaned_documents(
    dataset: DataSet, existing_uuids: set[str], status: Optional[str] = None
) -> dict[str, str]:
    """
    Return documents with specific status in RAGFlow
    but missing in database.
    If status is None, then  use all documents regardless
    of their status.

    Args:
        dataset: RAGFlow dataset object.
        existing_uuids: set of existing respository uuids.
        status: Filter by document status (e.g., "DONE"). None for all documents.

    Returns:
        dict: Mapping dict: Mapping of {document_id:uuid}
    """

    orphaned_documents_map = {}
    if dataset is None:
        return orphaned_documents_map

    documents_id_name_map = generate_ragflow_id_docname_map(
        dataset=dataset, status=status
    )

    for doc_id, doc_name in documents_id_name_map.items():
        doc_uuid = str(doc_name).replace(".pdf", "")
        if doc_uuid not in existing_uuids:
            orphaned_documents_map[doc_id] = doc_uuid

    return orphaned_documents_map


def generate_ragflow_id_docname_map(
    dataset: DataSet, status: Optional[str] = None
) -> dict:
    """
    Generate a mapping of RAGFlow document IDs with specific status
    to their PDF names. If status is None, then use all documents
    regardless of their status.

    Args:
        dataset: RAGFlow dataset object
        status: Filter by document status (e.g., "DONE"). None for all documents.

    Returns:
        dict: Mapping of {document_id: document_name}
    """
    document_id_to_name = {}

    if dataset is None:
        return document_id_to_name

    documents = get_all_documents(dataset=dataset)
    for document in documents:
        document_status = str(getattr(document, "run", None))
        if document_status == status or status is None:
            document_id_to_name[document.id] = document.name

    return document_id_to_name


def get_docs_names(
    dataset: DataSet, status: Optional[str] = None
) -> list[str]:
    """
    Extract all document names from a RAGFlow dataset with specific status.
    If status is None, then use all documents regardless od their status.

    This function retrieves the names of all documents currently stored in the
    provided RAGFlow dataset. It's commonly used to check which documents exist
    before uploading new ones or when displaying available documents.

    Args:
        dataset: RAGFlow DataSet object containing documents to list
        status: Filter by document status (e.g., "DONE"). None for all documents.

    Returns:
        list[str]: A list of document names (typically PDF filenames)
        from the dataset.
        Returns an empty list if the dataset contains no documents.
    """
    documents_names = []
    documents = get_all_documents(dataset=dataset)
    for document in documents:
        document_status = str(getattr(document, "run", None))
        if document_status == status or status is None:
            documents_names.append(document.name)

    return documents_names


def get_docs_ids(
    dataset: DataSet, statuses: Optional[List[str]] = None
) -> list[str]:
    """
    Extract all documents id from a RAGFlow dataset with specific status.

    Args:
        dataset: RAGFlow dataset object.
        statuses: list of status.

    Returns:
        list[str]: A list of document ids, from dataset.
        Returns an empty list if the dataset contains no documents with
        this status.
    """
    document_ids = []

    document_list = get_all_documents(dataset=dataset)
    if statuses is not None:
        for status in statuses:
            for document in document_list:
                document_status = str(getattr(document, "run", None))
                if document_status == status:
                    document_ids.append(document.id)
    else:
        # When no status filter is provided, return all document IDs
        for document in document_list:
            document_ids.append(document.id)

    return document_ids


def remove_temp_pdf(folder_path: str, processed_file_names: list[str]) -> bool:
    """
    Remove temporal pdf files after the parser has processed it.

    Args:
        folder_path: Directory path where the file will be saved.
        processed_file_names: file names list of the files with DONE
            status in RAGFlow.

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
