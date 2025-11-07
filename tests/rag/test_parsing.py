import asyncio
import threading
from unittest import TestCase, mock

from ingest_ragflow.rag import parsing as rp


class DummyDoc:
    def __init__(self, id, name, progress=0, run="RUNNING"):
        self.id = id
        self.name = name
        self.progress = progress
        self.run = run


class DummyDataset:
    def __init__(self):
        self._docs = []

    def upload_documents(self, docs):
        # Convert dicts to DummyDoc simulating real objects
        for doc in docs:
            doc_id = doc["displayed_name"].split(".")[0]
            self._docs.append(DummyDoc(doc_id, doc["displayed_name"]))

    def list_documents(self):
        return self._docs

    def async_parse_documents(self, ids):
        # Simulates document parsing
        for doc in self._docs:
            if doc["displayed_name"].split(".")[0] in ids:
                # Just update the doc in place, no need to create DummyDoc
                doc["progress"] = 1
                doc["run"] = "DONE"


class TestParsing(TestCase):
    def setUp(self):
        self.dataset = DummyDataset()
        self.lock = threading.Lock()  # real lock to supportt 'with'
        self.document_ids = []

    @mock.patch("ingest_ragflow.rag.parsing.generate_document_list")
    @mock.patch("tqdm.tqdm", lambda x, **kwargs: x)
    def test_upload_and_parse_file_success(self, mock_gen_docs):
        mock_gen_docs.return_value = [
            {"displayed_name": "file.pdf", "blob": b"data"}
        ]

        rp.upload_and_parse_file(
            "file.pdf",
            self.dataset,  # type: ignore
            self.lock,
            self.document_ids,
        )

        self.assertTrue(len(self.document_ids) > 0)

    @mock.patch("ingest_ragflow.rag.parsing.retrieve_item_file")
    @mock.patch("ingest_ragflow.rag.parsing.generate_document_list")
    @mock.patch("tqdm.tqdm", lambda x, **kwargs: x)
    def test_process_item_success(self, mock_gen_docs, mock_retrieve):
        mock_retrieve.return_value = ("file.pdf", {"uuid": "id1"})
        mock_gen_docs.return_value = [
            {"displayed_name": "file.pdf", "blob": b"data"}
        ]

        doc_id, metadata = rp.process_item(
            base_url="http://test-ri.com",
            base_url_rest="http://base-url-rest",
            item_id="id1",
            folder_path="/tmp",
            position=0,
            ragflow_dataset=self.dataset,  # type: ignore
            lock=self.lock,
            documents_ids=self.document_ids,
        )

        self.assertIsNotNone(doc_id)
        self.assertIsNotNone(metadata)
        if metadata is not None:
            self.assertEqual(metadata["uuid"], "id1")

    @mock.patch("ingest_ragflow.rag.parsing.get_items")
    @mock.patch("ingest_ragflow.rag.parsing.get_items_ids")
    @mock.patch("ingest_ragflow.rag.parsing.process_item")
    @mock.patch("tqdm.tqdm", lambda x, **kwargs: x)
    def test_process_items_in_parallel_success(
        self, mock_process_item, mock_get_ids, mock_get_items
    ):
        mock_get_items.return_value = [{"uuid": "id1"}]
        mock_get_ids.return_value = ["id1"]
        mock_process_item.return_value = ("doc_id1", {"uuid": "id1"})
        document_ids = []

        result = rp.process_items_in_parallel(
            base_url="http://test-ri.com",
            base_url_rest="http://base-url-rest",
            folder_path="/tmp",
            ragflow_dataset=self.dataset,  # type: ignore
            document_ids=document_ids,
            max_concurrent_tasks=2,
        )

        self.assertIn("doc_id1", result)
        doc_metadata = result.get("doc_id1")
        self.assertIsNotNone(doc_metadata)
        self.assertEqual(doc_metadata["uuid"], "id1")  # type: ignore

    def test_get_documents_map(self):
        doc_obj = DummyDoc("id1", "file1.pdf")
        self.dataset._docs = [doc_obj]
        result = rp.get_documents_map(self.dataset, ["id1"])  # type: ignore
        self.assertEqual(result, {"id1": "file1.pdf"})

    def test_monitor_parsing(self):
        doc_obj = DummyDoc("id1", "file1.pdf", progress=1, run="DONE")
        self.dataset._docs = [doc_obj]

        async def run_monitor():
            await rp.monitor_parsing(
                self.dataset,  # type: ignore
                ["id1"],
                poll_interval=0.01,
            )

        asyncio.run(run_monitor())


class TestFilterDoneDocuments(TestCase):
    def setUp(self):
        self.mock_dataset = mock.Mock()

    def test_filter_only_done_documents(self):
        # Create mock documents with different statuses
        doc1 = mock.Mock(id="doc1", run="DONE")
        doc2 = mock.Mock(id="doc2", run="PROCESSING")
        doc3 = mock.Mock(id="doc3", run="DONE")
        doc4 = mock.Mock(id="doc4", run="FAILED")

        self.mock_dataset.list_documents.return_value = [
            doc1,
            doc2,
            doc3,
            doc4,
        ]

        metadata_map = {
            "doc1": {"file": "file1.pdf", "status": "ready"},
            "doc2": {"file": "file2.pdf", "status": "pending"},
            "doc3": {"file": "file3.pdf", "status": "ready"},
            "doc4": {"file": "file4.pdf", "status": "error"},
        }

        result = rp.filter_done_documents(self.mock_dataset, metadata_map)

        # Only doc1 and doc3 should be in result
        self.assertEqual(len(result), 2)
        self.assertIn("doc1", result)
        self.assertIn("doc3", result)
        self.assertNotIn("doc2", result)
        self.assertNotIn("doc4", result)

    def test_empty_metadata_map(self):
        doc1 = mock.Mock(id="doc1", run="DONE")
        self.mock_dataset.list_documents.return_value = [doc1]

        result = rp.filter_done_documents(self.mock_dataset, {})

        self.assertEqual(result, {})

    def test_no_done_documents(self):
        doc1 = mock.Mock(id="doc1", run="PROCESSING")
        doc2 = mock.Mock(id="doc2", run="FAILED")

        self.mock_dataset.list_documents.return_value = [doc1, doc2]

        metadata_map = {
            "doc1": {"file": "file1.pdf"},
            "doc2": {"file": "file2.pdf"},
        }

        result = rp.filter_done_documents(self.mock_dataset, metadata_map)

        self.assertEqual(result, {})

    def test_all_documents_done(self):
        doc1 = mock.Mock(id="doc1", run="DONE")
        doc2 = mock.Mock(id="doc2", run="DONE")

        self.mock_dataset.list_documents.return_value = [doc1, doc2]

        metadata_map = {
            "doc1": {"file": "file1.pdf"},
            "doc2": {"file": "file2.pdf"},
        }

        result = rp.filter_done_documents(self.mock_dataset, metadata_map)

        self.assertEqual(result, metadata_map)
