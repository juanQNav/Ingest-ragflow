from ragflow_sdk.modules.dataset import DataSet

from ingest_ragflow.rag.files import get_all_documents


def display_final_summary(dataset: DataSet, metadata_map: dict) -> bool:
    """
    Display final summary of processed documents.

    Args:
        dataset:
        metadata_map:
    """
    try:
        documents = get_all_documents(dataset=dataset)
        documents = [doc for doc in documents if doc.id in metadata_map]
        print("\nFinal Summary: ")
        print("-" * 50)
        for doc in documents:
            print(
                f"{doc.name} | Status: {doc.run} |\
                    Fragments: {doc.chunk_count}"
            )
        print("-" * 50)
        print("Process completed successfully")
        return True
    except Exception as e:
        print(f"Could not retrieve final document status: {e}")
        return False
