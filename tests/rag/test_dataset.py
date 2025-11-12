import unittest
from unittest.mock import Mock, patch

from ragflow_sdk import RAGFlow
from ragflow_sdk.modules.dataset import DataSet

from ingest_ragflow.rag.dataset import get_dataset_by_id


class TestGetDatasetById(unittest.TestCase):
    def test_get_dataset_success(self):
        mock_rag = Mock(spec=RAGFlow)
        mock_dataset = Mock(spec=DataSet)
        mock_dataset.id = "test-dataset-123"
        mock_rag.list_datasets.return_value = [mock_dataset]

        result = get_dataset_by_id(mock_rag, "test-dataset-123")

        self.assertIsNotNone(result)
        if result is not None:
            self.assertEqual(result.id, "test-dataset-123")
        mock_rag.list_datasets.assert_called_once_with(id="test-dataset-123")

    def test_get_dataset_not_found(self):
        mock_rag = Mock(spec=RAGFlow)
        mock_rag.list_datasets.return_value = []

        result = get_dataset_by_id(mock_rag, "nonexistent-id")

        self.assertIsNone(result)
        mock_rag.list_datasets.assert_called_once_with(id="nonexistent-id")

    def test_get_dataset_returns_first_when_multiple(self):
        mock_rag = Mock(spec=RAGFlow)
        mock_dataset1 = Mock(spec=DataSet)
        mock_dataset1.id = "dataset-1"
        mock_dataset2 = Mock(spec=DataSet)
        mock_dataset2.id = "dataset-2"
        mock_rag.list_datasets.return_value = [mock_dataset1, mock_dataset2]

        result = get_dataset_by_id(mock_rag, "test-id")

        self.assertIs(result, mock_dataset1)
        if result is not None:
            self.assertEqual(result.id, "dataset-1")

    @patch("builtins.print")
    def test_get_dataset_handles_exception(self, mock_print):
        mock_rag = Mock(spec=RAGFlow)
        error_message = "Connection error"
        mock_rag.list_datasets.side_effect = Exception(error_message)

        result = get_dataset_by_id(mock_rag, "test-id")

        self.assertIsNone(result)
        mock_print.assert_called_once()
        call_args = mock_print.call_args[0][0]
        self.assertIn("Could not retrieve dataset test-id", call_args)
        self.assertIn(error_message, call_args)

    @patch("builtins.print")
    def test_get_dataset_handles_network_error(self, mock_print):
        mock_rag = Mock(spec=RAGFlow)
        mock_rag.list_datasets.side_effect = ConnectionError(
            "Network unavailable"
        )

        result = get_dataset_by_id(mock_rag, "test-id")

        self.assertIsNone(result)
        mock_print.assert_called_once()
