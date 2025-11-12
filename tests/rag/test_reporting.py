import unittest
from io import StringIO
from unittest.mock import Mock, patch

from ragflow_sdk.modules.dataset import DataSet

from ingest_ragflow.rag.reporting import display_final_summary


class TestDisplayFinalSummary(unittest.TestCase):
    @patch("sys.stdout", new_callable=StringIO)
    def test_display_summary_success(self, mock_stdout):
        mock_dataset = Mock(spec=DataSet)
        mock_doc1 = Mock()
        mock_doc1.id = "doc-1"
        mock_doc1.name = "Document1.pdf"
        mock_doc1.run = "Success"
        mock_doc1.chunk_count = 10

        mock_doc2 = Mock()
        mock_doc2.id = "doc-2"
        mock_doc2.name = "Document2.pdf"
        mock_doc2.run = "Completed"
        mock_doc2.chunk_count = 25

        mock_dataset.list_documents.return_value = [mock_doc1, mock_doc2]
        metadata_map = {"doc-1": {}, "doc-2": {}}

        result = display_final_summary(mock_dataset, metadata_map)

        self.assertTrue(result)
        mock_dataset.list_documents.assert_called_once()

        output = mock_stdout.getvalue()
        self.assertIn("Final Summary:", output)
        self.assertIn("Document1.pdf", output)
        self.assertIn("Success", output)
        self.assertIn("10", output)
        self.assertIn("Document2.pdf", output)
        self.assertIn("Completed", output)
        self.assertIn("25", output)
        self.assertIn("Process completed successfully", output)
        self.assertEqual(output.count("-" * 50), 2)

    @patch("sys.stdout", new_callable=StringIO)
    def test_display_summary_filters_documents(self, mock_stdout):
        mock_dataset = Mock(spec=DataSet)
        mock_doc1 = Mock()
        mock_doc1.id = "doc-1"
        mock_doc1.name = "Included.pdf"
        mock_doc1.run = "Success"
        mock_doc1.chunk_count = 5

        mock_doc2 = Mock()
        mock_doc2.id = "doc-2"
        mock_doc2.name = "NotIncluded.pdf"
        mock_doc2.run = "Success"
        mock_doc2.chunk_count = 3

        mock_dataset.list_documents.return_value = [mock_doc1, mock_doc2]
        metadata_map = {"doc-1": {}}  # Only doc-1 in metadata_map

        result = display_final_summary(mock_dataset, metadata_map)

        self.assertTrue(result)
        output = mock_stdout.getvalue()
        self.assertIn("Included.pdf", output)
        self.assertNotIn("NotIncluded.pdf", output)

    @patch("sys.stdout", new_callable=StringIO)
    def test_display_summary_empty_metadata_map(self, mock_stdout):
        mock_dataset = Mock(spec=DataSet)
        mock_doc = Mock()
        mock_doc.id = "doc-1"
        mock_doc.name = "Document.pdf"
        mock_doc.run = "Success"
        mock_doc.chunk_count = 10

        mock_dataset.list_documents.return_value = [mock_doc]
        metadata_map = {}  # Empty map

        result = display_final_summary(mock_dataset, metadata_map)

        self.assertTrue(result)
        output = mock_stdout.getvalue()
        self.assertIn("Final Summary:", output)
        self.assertNotIn("Document.pdf", output)
        self.assertIn("Process completed successfully", output)

    @patch("sys.stdout", new_callable=StringIO)
    def test_display_summary_no_documents(self, mock_stdout):
        mock_dataset = Mock(spec=DataSet)
        mock_dataset.list_documents.return_value = []
        metadata_map = {"doc-1": {}}

        result = display_final_summary(mock_dataset, metadata_map)

        self.assertTrue(result)
        output = mock_stdout.getvalue()
        self.assertIn("Final Summary:", output)
        self.assertIn("Process completed successfully", output)

    @patch("sys.stdout", new_callable=StringIO)
    def test_display_summary_handles_exception(self, mock_stdout):
        mock_dataset = Mock(spec=DataSet)
        error_message = "API connection failed"
        mock_dataset.list_documents.side_effect = Exception(error_message)
        metadata_map = {"doc-1": {}}

        result = display_final_summary(mock_dataset, metadata_map)

        self.assertFalse(result)
        output = mock_stdout.getvalue()
        self.assertIn("Could not retrieve final document status:", output)
        self.assertIn(error_message, output)

    @patch("sys.stdout", new_callable=StringIO)
    def test_display_summary_handles_attribute_error(self, mock_stdout):
        mock_dataset = Mock(spec=DataSet)
        mock_dataset.list_documents.side_effect = AttributeError(
            "Missing attribute"
        )
        metadata_map = {"doc-1": {}}

        result = display_final_summary(mock_dataset, metadata_map)

        self.assertFalse(result)
        output = mock_stdout.getvalue()
        self.assertIn("Could not retrieve final document status:", output)
