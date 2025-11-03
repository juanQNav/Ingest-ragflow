from unittest import TestCase, mock

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
        self.assertEqual(result[0]["displayed_name"], "file1.pdf")
        self.assertEqual(result[0]["blob"], b"data1")
        self.assertEqual(result[1]["displayed_name"], "file2.pdf")
        self.assertEqual(result[1]["blob"], b"data2")

    @mock.patch("ingest_ragflow.rag.files.read_binary_file")
    @mock.patch("os.path.basename", side_effect=lambda x: x.split("/")[-1])
    def test_process_files_in_parallel(self, mock_basename, mock_read):
        mock_read.side_effect = [b"data1", b"data2"]
        pdf_files = ["/path/file1.pdf", "/path/file2.pdf"]
        result = rf.process_files_in_parallel(pdf_files)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["displayed_name"], "file1.pdf")
        self.assertEqual(result[0]["blob"], b"data1")
        self.assertEqual(result[1]["displayed_name"], "file2.pdf")
        self.assertEqual(result[1]["blob"], b"data2")
