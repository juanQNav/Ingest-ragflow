import threading
from unittest import IsolatedAsyncioTestCase, TestCase, mock

from ingest_ragflow.rag import parsing as rp


class DummyDoc:
    def __init__(
        self, id: str, name: str, progress: float = 0.0, run: str = "RUNNING"
    ):
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

    @mock.patch("ingest_ragflow.rag.parsing.get_all_documents")
    def test_get_documents_map(self, mock_get_all_docs):
        doc_obj = DummyDoc("id1", "file1.pdf")
        mock_get_all_docs.return_value = [doc_obj]
        result = rp.get_documents_map(self.dataset, ["id1"])  # type: ignore
        self.assertEqual(result, {"id1": "file1.pdf"})


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


class TestMonitorParsing(IsolatedAsyncioTestCase):
    def setUp(self):
        self.mock_dataset = mock.Mock()
        self.document_ids = ["doc1", "doc2", "doc3"]
        self.callback_calls = []

    async def dummy_callback(self, doc_id, doc_name, status):
        self.callback_calls.append(
            {"doc_id": doc_id, "doc_name": doc_name, "status": status}
        )

    @mock.patch("ingest_ragflow.rag.parsing.get_all_documents")
    @mock.patch("tqdm.tqdm")
    async def test_monitor_parsing_all_documents_complete(
        self, mock_tqdm, mock_get_docs
    ):
        # Setup: documents start running, then complete
        running_docs = [
            DummyDoc("doc1", "file1.pdf", 0.5, "RUNNING"),
            DummyDoc("doc2", "file2.pdf", 0.3, "RUNNING"),
            DummyDoc("doc3", "file3.pdf", 0.7, "RUNNING"),
        ]

        done_docs = [
            DummyDoc("doc1", "file1.pdf", 1.0, "DONE"),
            DummyDoc("doc2", "file2.pdf", 1.0, "DONE"),
            DummyDoc("doc3", "file3.pdf", 1.0, "DONE"),
        ]

        # First call returns running, second and third call returns done
        # Note: get_all_documents is called twice - once in get_documents_map
        # and then in the monitoring loop
        mock_get_docs.side_effect = [
            # First call in get_documents_map
            [
                DummyDoc("doc1", "file1.pdf"),
                DummyDoc("doc2", "file2.pdf"),
                DummyDoc("doc3", "file3.pdf"),
            ],
            # First monitoring loop call
            running_docs,
            # Second monitoring loop call
            done_docs,
        ]

        # Mock progress bars
        mock_tqdm.return_value.__enter__ = mock.Mock()
        mock_tqdm.return_value.__exit__ = mock.Mock()
        mock_tqdm.return_value.n = 0
        mock_tqdm.return_value.refresh = mock.Mock()
        mock_tqdm.return_value.close = mock.Mock()

        await rp.monitor_parsing(
            dataset=self.mock_dataset,
            document_ids=self.document_ids,
            poll_interval=0.01,
            on_document_done=self.dummy_callback,
        )

        # Verify callback was called for each document
        self.assertEqual(len(self.callback_calls), 3)
        self.assertEqual(self.callback_calls[0]["doc_id"], "doc1")
        self.assertEqual(self.callback_calls[1]["doc_id"], "doc2")
        self.assertEqual(self.callback_calls[2]["doc_id"], "doc3")

        # Verify all callbacks have DONE status
        for call in self.callback_calls:
            self.assertEqual(call["status"], "DONE")

    @mock.patch("ingest_ragflow.rag.parsing.get_all_documents")
    @mock.patch("tqdm.tqdm")
    async def test_monitor_parsing_callback_called_once_per_document(
        self, mock_tqdm, mock_get_docs
    ):
        # Setup: documents complete on first poll but we poll multiple times
        done_docs = [
            DummyDoc("doc1", "file1.pdf", 1.0, "DONE"),
            DummyDoc("doc2", "file2.pdf", 1.0, "DONE"),
        ]

        # Return same done documents multiple times
        # First call for get_documents_map, then for monitoring loops
        mock_get_docs.side_effect = [
            [DummyDoc("doc1", "file1.pdf"), DummyDoc("doc2", "file2.pdf")],
            done_docs,
        ]

        mock_tqdm.return_value.n = 0
        mock_tqdm.return_value.refresh = mock.Mock()
        mock_tqdm.return_value.close = mock.Mock()

        await rp.monitor_parsing(
            dataset=self.mock_dataset,
            document_ids=["doc1", "doc2"],
            poll_interval=0.01,
            on_document_done=self.dummy_callback,
        )

        # Callback should be called exactly once per document
        self.assertEqual(len(self.callback_calls), 2)
        doc_ids = [call["doc_id"] for call in self.callback_calls]
        self.assertEqual(set(doc_ids), {"doc1", "doc2"})

    @mock.patch("ingest_ragflow.rag.parsing.get_all_documents")
    @mock.patch("tqdm.tqdm")
    async def test_monitor_parsing_without_callback(
        self, mock_tqdm, mock_get_docs
    ):
        done_docs = [
            DummyDoc("doc1", "file1.pdf", 1.0, "DONE"),
        ]

        # First call for get_documents_map, second for monitoring
        mock_get_docs.side_effect = [
            [DummyDoc("doc1", "file1.pdf")],
            done_docs,
        ]

        mock_tqdm.return_value.n = 0
        mock_tqdm.return_value.refresh = mock.Mock()
        mock_tqdm.return_value.close = mock.Mock()

        # Should not raise error when callback is None
        await rp.monitor_parsing(
            dataset=self.mock_dataset,
            document_ids=["doc1"],
            poll_interval=0.01,
            on_document_done=None,
        )

        # No callback calls should have been made
        self.assertEqual(len(self.callback_calls), 0)

    @mock.patch("ingest_ragflow.rag.parsing.get_all_documents")
    @mock.patch("tqdm.tqdm")
    async def test_monitor_parsing_network_error_recovery(
        self, mock_tqdm, mock_get_docs
    ):
        import requests.exceptions

        # First call for get_documents_map, then error, then success
        mock_get_docs.side_effect = [
            [DummyDoc("doc1", "file1.pdf")],
            requests.exceptions.ConnectionError("Network error"),
            [DummyDoc("doc1", "file1.pdf", 1.0, "DONE")],
        ]

        mock_tqdm.return_value.n = 0
        mock_tqdm.return_value.refresh = mock.Mock()
        mock_tqdm.return_value.close = mock.Mock()

        await rp.monitor_parsing(
            dataset=self.mock_dataset,
            document_ids=["doc1"],
            poll_interval=0.01,
            retry_delay=0.01,
            max_retries=5,
            on_document_done=self.dummy_callback,
        )

        # Callback should still be called after recovery
        self.assertEqual(len(self.callback_calls), 1)
        self.assertEqual(self.callback_calls[0]["doc_id"], "doc1")

    @mock.patch("ingest_ragflow.rag.parsing.get_all_documents")
    @mock.patch("tqdm.tqdm")
    async def test_monitor_parsing_progressive_completion(
        self, mock_tqdm, mock_get_docs
    ):
        # Documents complete one at a time
        poll_1 = [
            DummyDoc("doc1", "file1.pdf", 0.5, "RUNNING"),
            DummyDoc("doc2", "file2.pdf", 0.3, "RUNNING"),
        ]

        poll_2 = [
            DummyDoc("doc1", "file1.pdf", 1.0, "DONE"),  # First completes
            DummyDoc("doc2", "file2.pdf", 0.7, "RUNNING"),
        ]

        poll_3 = [
            DummyDoc("doc1", "file1.pdf", 1.0, "DONE"),
            DummyDoc("doc2", "file2.pdf", 1.0, "DONE"),  # Second completes
        ]

        # First for get_documents_map, then monitoring loops
        mock_get_docs.side_effect = [
            [DummyDoc("doc1", "file1.pdf"), DummyDoc("doc2", "file2.pdf")],
            poll_1,
            poll_2,
            poll_3,
        ]

        mock_tqdm.return_value.n = 0
        mock_tqdm.return_value.refresh = mock.Mock()
        mock_tqdm.return_value.close = mock.Mock()

        await rp.monitor_parsing(
            dataset=self.mock_dataset,
            document_ids=["doc1", "doc2"],
            poll_interval=0.01,
            on_document_done=self.dummy_callback,
        )

        # Both callbacks should be called
        self.assertEqual(len(self.callback_calls), 2)

        # Verify callback was called in order of completion
        self.assertEqual(self.callback_calls[0]["doc_id"], "doc1")
        self.assertEqual(self.callback_calls[1]["doc_id"], "doc2")

    @mock.patch("ingest_ragflow.rag.parsing.get_all_documents")
    @mock.patch("tqdm.tqdm")
    async def test_monitor_parsing_max_retries_exceeded(
        self, mock_tqdm, mock_get_docs
    ):
        import requests.exceptions

        # First call succeeds for get_documents_map, then all fail in monitoring loop
        # Plus one final attempt to check status at the end
        mock_get_docs.side_effect = [
            [DummyDoc("doc1", "file1.pdf")],  # get_documents_map
            requests.exceptions.ConnectionError(
                "Network error"
            ),  # initial attempt
            requests.exceptions.ConnectionError("Network error"),  # retry 1
            requests.exceptions.ConnectionError("Network error"),  # retry 2
            requests.exceptions.ConnectionError(
                "Network error"
            ),  # final status check
        ]

        mock_tqdm.return_value.n = 0
        mock_tqdm.return_value.refresh = mock.Mock()
        mock_tqdm.return_value.close = mock.Mock()

        await rp.monitor_parsing(
            dataset=self.mock_dataset,
            document_ids=["doc1"],
            poll_interval=0.01,
            retry_delay=0.01,
            max_retries=2,  # Low retry count for testing
            on_document_done=self.dummy_callback,
        )

        # No callback should be called since monitoring failed
        self.assertEqual(len(self.callback_calls), 0)

        # Verify get_all_documents was called:
        # 1 for get_documents_map + 3 for monitoring (initial + 2 retries) + 1 for final check
        self.assertEqual(mock_get_docs.call_count, 5)

    @mock.patch("ingest_ragflow.rag.parsing.get_all_documents")
    @mock.patch("tqdm.tqdm")
    async def test_monitor_parsing_callback_exception_handling(
        self, mock_tqdm, mock_get_docs
    ):
        async def failing_callback(doc_id, doc_name, status):
            self.callback_calls.append(doc_id)
            if doc_id == "doc1":
                raise ValueError("Callback error")

        done_docs = [
            DummyDoc("doc1", "file1.pdf", 1.0, "DONE"),
            DummyDoc("doc2", "file2.pdf", 1.0, "DONE"),
        ]

        # First for get_documents_map, then for monitoring (2 attempts due to retry)
        mock_get_docs.side_effect = [
            [DummyDoc("doc1", "file1.pdf"), DummyDoc("doc2", "file2.pdf")],
            done_docs,  # First attempt - callback will raise ValueError
            done_docs,  # Retry after ValueError
        ]

        mock_tqdm.return_value.n = 0
        mock_tqdm.return_value.refresh = mock.Mock()
        mock_tqdm.return_value.close = mock.Mock()

        # The implementation catches callback exceptions and treats them as network errors
        # It will retry and eventually complete
        await rp.monitor_parsing(
            dataset=self.mock_dataset,
            document_ids=["doc1", "doc2"],
            poll_interval=0.01,
            retry_delay=0.01,
            on_document_done=failing_callback,
        )

        # The callback was called multiple times due to the error and retry
        self.assertGreater(len(self.callback_calls), 0)

    @mock.patch("ingest_ragflow.rag.parsing.get_all_documents")
    @mock.patch("tqdm.tqdm")
    async def test_monitor_parsing_empty_document_list(
        self, mock_tqdm, mock_get_docs
    ):
        # get_all_documents returns empty list
        mock_get_docs.return_value = []

        mock_tqdm.return_value.n = 0
        mock_tqdm.return_value.refresh = mock.Mock()
        mock_tqdm.return_value.close = mock.Mock()

        await rp.monitor_parsing(
            dataset=self.mock_dataset,
            document_ids=[],
            poll_interval=0.01,
            on_document_done=self.dummy_callback,
        )

        # No callbacks should be made for empty list
        self.assertEqual(len(self.callback_calls), 0)
