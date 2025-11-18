import tempfile
from io import StringIO
from pathlib import Path
from unittest import TestCase, mock
from unittest.mock import patch

from ingest_ragflow.rag import files as rf


class TestRagFiles(TestCase):
    @mock.patch(
        "builtins.open",
        new_callable=mock.mock_open,
        read_data=b"dummy content",
    )
    def test_read_binary_file(self, mock_file):
        content = rf.read_binary_file("fake_path.pdf")
        self.assertEqual(content, b"dummy content")
        mock_file.assert_called_once_with("fake_path.pdf", "rb")

    @mock.patch("os.listdir")
    @mock.patch("os.path.join", side_effect=lambda a, b: f"{a}/{b}")
    def test_find_pdf_files(self, mock_join, mock_listdir):
        mock_listdir.return_value = ["doc1.pdf", "doc2.txt", "doc3.pdf"]
        result = rf.find_pdf_files("/fake/dir")
        self.assertEqual(result, ["/fake/dir/doc1.pdf", "/fake/dir/doc3.pdf"])

    @mock.patch("ingest_ragflow.rag.files.read_binary_file")
    @mock.patch("os.path.basename", side_effect=lambda x: x.split("/")[-1])
    def test_generate_document_list(self, mock_basename, mock_read):
        mock_read.side_effect = [b"data1", b"data2"]
        files = ["/path/file1.pdf", "/path/file2.pdf"]
        result = rf.generate_document_list(files)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["display_name"], "file1.pdf")
        self.assertEqual(result[0]["blob"], b"data1")
        self.assertEqual(result[1]["display_name"], "file2.pdf")
        self.assertEqual(result[1]["blob"], b"data2")

    @mock.patch("ingest_ragflow.rag.files.read_binary_file")
    @mock.patch("os.path.basename", side_effect=lambda x: x.split("/")[-1])
    def test_process_files_in_parallel(self, mock_basename, mock_read):
        mock_read.side_effect = [b"data1", b"data2"]
        pdf_files = ["/path/file1.pdf", "/path/file2.pdf"]
        result = rf.process_files_in_parallel(pdf_files)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["display_name"], "file1.pdf")
        self.assertEqual(result[0]["blob"], b"data1")
        self.assertEqual(result[1]["display_name"], "file2.pdf")
        self.assertEqual(result[1]["blob"], b"data2")

    def test_generate_ragflow_id_docname_map_with_multiple_documents(self):
        mock_dataset = mock.Mock()
        mock_doc1 = mock.Mock()
        mock_doc1.name = "document1.pdf"
        mock_doc1.id = "testid1"
        mock_doc1.run = "DONE"
        mock_doc2 = mock.Mock()
        mock_doc2.name = "document2.pdf"
        mock_doc2.id = "testid2"
        mock_doc2.run = "DONE"
        mock_doc3 = mock.Mock()
        mock_doc3.name = "report.pdf"
        mock_doc3.id = "testid3"
        mock_doc3.run = "CANCEL"

        mock_dataset.list_documents.return_value = [
            mock_doc1,
            mock_doc2,
            mock_doc3,
        ]

        result = rf.generate_ragflow_id_docname_map(
            mock_dataset, status="DONE"
        )

        assert result == {
            "testid1": "document1.pdf",
            "testid2": "document2.pdf",
        }
        self.assertEqual(len(result.items()), 2)

        mock_dataset.list_documents.assert_called_once_with(
            keywords=None,
            page=1,
            page_size=100,
            orderby="create_time",
            desc=True,
        )

    def test_generate_ragflow_id_docname_map_with_empty_dataset(self):
        mock_dataset = mock.Mock()
        mock_dataset.list_documents.return_value = []

        result = rf.generate_ragflow_id_docname_map(mock_dataset)

        assert result == {}
        assert len(result) == 0
        mock_dataset.list_documents.assert_called_once_with(
            keywords=None,
            page=1,
            page_size=100,
            orderby="create_time",
            desc=True,
        )

    def test_get_docs_names_with_multiple_documents(self):
        mock_dataset = mock.Mock()
        mock_doc1 = mock.Mock()
        mock_doc1.name = "document1.pdf"
        mock_doc1.run = "DONE"
        mock_doc2 = mock.Mock()
        mock_doc2.name = "document2.pdf"
        mock_doc2.run = "DONE"
        mock_doc3 = mock.Mock()
        mock_doc3.name = "report.pdf"
        mock_doc3.run = "CANCEL"

        mock_dataset.list_documents.return_value = [
            mock_doc1,
            mock_doc2,
            mock_doc3,
        ]

        result = rf.get_docs_names(mock_dataset, status="DONE")

        self.assertEqual(result, ["document1.pdf", "document2.pdf"])
        self.assertEqual(len(result), 2)
        mock_dataset.list_documents.assert_called_once_with(
            keywords=None,
            page=1,
            page_size=100,
            orderby="create_time",
            desc=True,
        )

    def test_get_docs_names_with_empty_dataset(self):
        mock_dataset = mock.Mock()
        mock_dataset.list_documents.return_value = []

        result = rf.get_docs_names(mock_dataset)

        assert result == []
        assert len(result) == 0
        mock_dataset.list_documents.assert_called_once_with(
            keywords=None,
            page=1,
            page_size=100,
            orderby="create_time",
            desc=True,
        )

    @mock.patch("ingest_ragflow.rag.files.generate_ragflow_id_docname_map")
    def test_get_orphaned_documents_with_multiple_documents_all_exist_in_db(
        self, mock_generate_map
    ):
        mock_dataset = mock.Mock()

        mock_generate_map.return_value = {
            "fake-uuid1": "uuid1.pdf",
            "fake-uuid2": "uuid2.pdf",
            "fake-uuid3": "uuid3.pdf",
        }

        existing_uuids = {"uuid1", "uuid2", "uuid3"}

        orphaned_documents = rf.get_orphaned_documents(
            dataset=mock_dataset, existing_uuids=existing_uuids
        )

        mock_generate_map.assert_called_once_with(
            dataset=mock_dataset, status=None
        )

        expected_result = {}

        self.assertEqual(orphaned_documents, expected_result)

    @mock.patch("ingest_ragflow.rag.files.generate_ragflow_id_docname_map")
    def test_get_orphaned_documents_with_partial_match(
        self, mock_generate_map
    ):
        mock_dataset = mock.Mock()

        mock_generate_map.return_value = {
            "fake-uuid1": "uuid1.pdf",
            "fake-uuid2": "uuid2.pdf",
            "fake-uuid3": "uuid3.pdf",
        }

        existing_uuids = {"uuid1", "uuid3"}

        orphaned_documents = rf.get_orphaned_documents(
            dataset=mock_dataset, existing_uuids=existing_uuids
        )

        mock_generate_map.assert_called_once_with(
            dataset=mock_dataset, status=None
        )

        expected_result = {
            "fake-uuid2": "uuid2",
        }

        self.assertEqual(orphaned_documents, expected_result)

    def test_get_orphaned_documents_returns_empty_dict_when_dataset_is_none(
        self,
    ):
        orphaned_documents = rf.get_orphaned_documents(
            dataset=None,  # type: ignore[arg-type]
            existing_uuids=set(),
        )

        expected_result = {}
        self.assertDictEqual(orphaned_documents, expected_result)

    @mock.patch("ingest_ragflow.rag.files.generate_ragflow_id_docname_map")
    def test_get_orphaned_documents_empty_when_no_documents(
        self, mock_generate_map
    ):
        mock_dataset = mock.Mock()

        mock_generate_map.return_value = {}

        existing_uuids = {"uuid1", "uuid2"}

        orphaned_documents = rf.get_orphaned_documents(
            dataset=mock_dataset, existing_uuids=existing_uuids
        )

        expected_result = {}

        self.assertEqual(orphaned_documents, expected_result)

    @mock.patch("ingest_ragflow.rag.files.generate_ragflow_id_docname_map")
    def test_get_orphaned_documents_all_when_not_in_db(
        self, mock_generate_map
    ):
        mock_dataset = mock.Mock()

        mock_generate_map.return_value = {
            "fake-uuid1": "uuid11.pdf",
            "fake-uuid2": "uuid22.pdf",
            "fake-uuid3": "uuid33.pdf",
        }

        existing_uuids = {"uuid1", "uuid2", "uuid3"}

        orphaned_documents = rf.get_orphaned_documents(
            dataset=mock_dataset, existing_uuids=existing_uuids
        )

        expected_result = {
            "fake-uuid1": "uuid11",
            "fake-uuid2": "uuid22",
            "fake-uuid3": "uuid33",
        }

        self.assertEqual(orphaned_documents, expected_result)

    def test_rename_document_with_extension(self):
        mock_doc1 = mock.Mock()
        mock_doc1.name = "doc1.pdf"
        mock_doc1.update = mock.Mock(return_value=None)
        name = "fake-uuid"

        def update_sied_effect(data):
            mock_doc1.name = data["name"]

        mock_doc1.update.side_effect = update_sied_effect

        result = rf.rename_document_name(mock_doc1, name)

        self.assertTrue(result)
        self.assertEqual(mock_doc1.name, f"{name}.pdf")
        mock_doc1.update.assert_called_once_with({"name": f"{name}.pdf"})

    def test_get_docs_ids_with_no_status_filter_returns_all_documents(self):
        mock_dataset = mock.Mock()
        mock_doc1 = mock.Mock(id="doc-id-1", run="UNSTART")
        mock_doc2 = mock.Mock(id="doc-id-2", run="FAIL")
        mock_doc3 = mock.Mock(id="doc-id-3", run="RUNNING")
        mock_dataset.list_documents.return_value = [
            mock_doc1,
            mock_doc2,
            mock_doc3,
        ]

        result = rf.get_docs_ids(dataset=mock_dataset, statuses=None)

        self.assertEqual(len(result), 3)
        self.assertIn("doc-id-1", result)
        self.assertIn("doc-id-2", result)
        self.assertIn("doc-id-3", result)
        mock_dataset.list_documents.assert_called_once_with(
            keywords=None,
            page=1,
            page_size=100,
            orderby="create_time",
            desc=True,
        )

    def test_get_docs_ids_with_single_status_filter_returns_matching_documents(
        self,
    ):
        mock_dataset = mock.Mock()
        mock_doc1 = mock.Mock(id="doc-id-1", run="DONE")
        mock_doc2 = mock.Mock(id="doc-id-2", run="FAIL")
        mock_doc3 = mock.Mock(id="doc-id-3", run="DONE")
        mock_dataset.list_documents.return_value = [
            mock_doc1,
            mock_doc2,
            mock_doc3,
        ]

        result = rf.get_docs_ids(dataset=mock_dataset, statuses=["DONE"])

        self.assertEqual(len(result), 2)
        self.assertIn("doc-id-1", result)
        self.assertIn("doc-id-3", result)
        self.assertNotIn("doc-id-2", result)
        mock_dataset.list_documents.assert_called_once_with(
            keywords=None,
            page=1,
            page_size=100,
            orderby="create_time",
            desc=True,
        )

    def test_get_docs_ids_with_multiple_statuses_returns_matching_documents(
        self,
    ):
        mock_dataset = mock.Mock()
        mock_doc1 = mock.Mock(id="doc-id-1", run="DONE")
        mock_doc2 = mock.Mock(id="doc-id-2", run="FAIL")
        mock_doc3 = mock.Mock(id="doc-id-3", run="RUNNING")
        mock_doc4 = mock.Mock(id="doc-id-4", run="DONE")
        mock_dataset.list_documents.return_value = [
            mock_doc1,
            mock_doc2,
            mock_doc3,
            mock_doc4,
        ]

        result = rf.get_docs_ids(
            dataset=mock_dataset, statuses=["DONE", "FAIL"]
        )

        self.assertIn("doc-id-1", result)
        self.assertIn("doc-id-2", result)
        self.assertIn("doc-id-4", result)
        self.assertNotIn("doc-id-3", result)
        mock_dataset.list_documents.assert_called_once_with(
            keywords=None,
            page=1,
            page_size=100,
            orderby="create_time",
            desc=True,
        )

    def test_get_docs_ids_with_no_matching_status_returns_empty_list(self):
        mock_dataset = mock.Mock()
        mock_doc1 = mock.Mock(id="doc-id-1", run="DONE")
        mock_doc2 = mock.Mock(id="doc-id-2", run="FAIL")
        mock_dataset.list_documents.return_value = [mock_doc1, mock_doc2]

        result = rf.get_docs_ids(dataset=mock_dataset, statuses=["CANCEL"])

        self.assertEqual(len(result), 0)
        self.assertEqual(result, [])
        mock_dataset.list_documents.assert_called_once_with(
            keywords=None,
            page=1,
            page_size=100,
            orderby="create_time",
            desc=True,
        )

    def test_get_docs_ids_with_empty_dataset_returns_empty_list(self):
        mock_dataset = mock.Mock()
        mock_dataset.list_documents.return_value = []

        result = rf.get_docs_ids(dataset=mock_dataset, statuses=["DONE"])

        self.assertEqual(len(result), 0)
        self.assertEqual(result, [])
        mock_dataset.list_documents.assert_called_once_with(
            keywords=None,
            page=1,
            page_size=100,
            orderby="create_time",
            desc=True,
        )

    def test_get_docs_ids_with_empty_dataset_and_no_status_returns_empty_list(
        self,
    ):
        mock_dataset = mock.Mock()
        mock_dataset.list_documents.return_value = []

        result = rf.get_docs_ids(dataset=mock_dataset, statuses=None)

        self.assertEqual(len(result), 0)
        self.assertEqual(result, [])
        mock_dataset.list_documents.assert_called_once_with(
            keywords=None,
            page=1,
            page_size=100,
            orderby="create_time",
            desc=True,
        )

    def test_returns_empty_list_when_dataset_is_none(self):
        result = rf.get_all_documents(dataset=None)  # type: ignore[arg-type]
        self.assertEqual(result, [])

    def test_single_page_retrieval(self):
        mock_dataset = mock.Mock()
        mock_docs = [{"id": 1}, {"id": 2}, {"id": 3}]
        mock_dataset.list_documents.return_value = mock_docs

        result = rf.get_all_documents(mock_dataset, page_size=10)

        self.assertEqual(result, mock_docs)
        mock_dataset.list_documents.assert_called_once_with(
            keywords=None,
            page=1,
            page_size=10,
            orderby="create_time",
            desc=True,
        )

    def test_multiple_page_retrieval(self):
        mock_dataset = mock.Mock()
        page1_docs = [{"id": i} for i in range(1, 11)]  # 10 docs
        page2_docs = [{"id": i} for i in range(11, 21)]  # 10 docs
        page3_docs = [{"id": i} for i in range(21, 26)]  # 5 docs (last page)
        mock_dataset.list_documents.side_effect = [
            page1_docs,
            page2_docs,
            page3_docs,
        ]

        result = rf.get_all_documents(mock_dataset, page_size=10)

        expected_docs = page1_docs + page2_docs + page3_docs
        self.assertEqual(result, expected_docs)
        self.assertEqual(mock_dataset.list_documents.call_count, 3)
        mock_dataset.list_documents.assert_any_call(
            keywords=None,
            page=1,
            page_size=10,
            orderby="create_time",
            desc=True,
        )
        mock_dataset.list_documents.assert_any_call(
            keywords=None,
            page=2,
            page_size=10,
            orderby="create_time",
            desc=True,
        )
        mock_dataset.list_documents.assert_any_call(
            keywords=None,
            page=3,
            page_size=10,
            orderby="create_time",
            desc=True,
        )

    def test_stops_when_empty_page_received(self):
        mock_dataset = mock.Mock()
        mock_dataset.list_documents.return_value = []

        result = rf.get_all_documents(mock_dataset)

        self.assertEqual(result, [])
        mock_dataset.list_documents.assert_called_once()

    def test_stops_when_partial_page_received(self):
        mock_dataset = mock.Mock()
        page1_docs = [{"id": i} for i in range(1, 101)]  # 100 docs (full page)
        page2_docs = [
            {"id": i} for i in range(101, 151)
        ]  # 50 docs (partial page)
        mock_dataset.list_documents.side_effect = [page1_docs, page2_docs]

        result = rf.get_all_documents(mock_dataset, page_size=100)

        expected_docs = page1_docs + page2_docs
        self.assertEqual(result, expected_docs)
        self.assertEqual(mock_dataset.list_documents.call_count, 2)
        mock_dataset.list_documents.assert_any_call(
            keywords=None,
            page=1,
            page_size=100,
            orderby="create_time",
            desc=True,
        )
        mock_dataset.list_documents.assert_any_call(
            keywords=None,
            page=2,
            page_size=100,
            orderby="create_time",
            desc=True,
        )

    def test_handles_exception_gracefully(self):
        mock_dataset = mock.Mock()
        page1_docs = [{"id": 1}, {"id": 2}]
        mock_dataset.list_documents.side_effect = [
            page1_docs,
            Exception("Connection error"),
        ]

        result = rf.get_all_documents(mock_dataset, verbose=False)

        self.assertEqual(result, page1_docs)
        self.assertEqual(mock_dataset.list_documents.call_count, 1)

    @patch("builtins.print")
    def test_verbose_mode_prints_error(self, mock_print):
        mock_dataset = mock.Mock()
        mock_dataset.list_documents.side_effect = Exception("API error")

        rf.get_all_documents(mock_dataset, verbose=True)

        self.assertTrue(
            any("API error" in str(call) for call in mock_print.call_args_list)
        )


class TestRemoveFiles(TestCase):
    def test_remove_single_existing_file(self):
        with tempfile.TemporaryDirectory() as tmp_path:
            # Create a test PDF file
            test_file = Path(tmp_path) / "test.pdf"
            test_file.write_text("dummy pdf content")

            self.assertTrue(test_file.exists())
            result = rf.remove_temp_pdf(tmp_path, ["test.pdf"])

            self.assertTrue(result)
            self.assertFalse(test_file.exists())

    def test_remove_multiple_existing_files(self):
        with tempfile.TemporaryDirectory() as tmp_path:
            # Create multiple test files
            files = ["file1.pdf", "file2.pdf", "file3.pdf"]
            for filename in files:
                (Path(tmp_path) / filename).write_text("content")

            result = rf.remove_temp_pdf(tmp_path, files)

            self.assertTrue(result)
            for filename in files:
                self.assertFalse((Path(tmp_path) / filename).exists())

    def test_file_does_not_exist(self):
        with tempfile.TemporaryDirectory() as tmp_path:
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                result = rf.remove_temp_pdf(tmp_path, ["nonexistent.pdf"])

                captured = mock_stdout.getvalue()
                self.assertTrue(result)
                self.assertIn("does not exists", captured)
                self.assertIn("skipping", captured)
