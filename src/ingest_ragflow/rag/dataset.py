from typing import Optional

from ragflow_sdk import RAGFlow
from ragflow_sdk.modules.dataset import DataSet


def get_dataset_by_id(
    rag_object: RAGFlow, dataset_id: str
) -> Optional[DataSet]:
    """
    Get RAGFlow dataset by dataset ID.

    Args:
        rag_object: RAGFlow client instance
        dataset_id: ID of the dataset to retrieve

    Returns:
        DataSet object if found, None otherwise
    """
    try:
        datasets = rag_object.list_datasets(id=dataset_id)
        if datasets:
            return datasets[0]
        return None
    except Exception as e:
        print(f"Could not retrieve dataset {dataset_id}: {e}")
