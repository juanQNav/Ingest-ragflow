from unittest import TestCase, mock

from ingest_ragflow.dspace_api import files as f


class TestFiles(TestCase):
    def setUp(self) -> None:
        self.base_url = "http://test-ri.com"
        self.base_url_rest = "http://base-url-rest"

    @mock.patch("ingest_ragflow.dspace_api.files.requests.get")
    @mock.patch("builtins.open", new_callable=mock.mock_open)
    @mock.patch("tqdm.tqdm", lambda x, **kwargs: x)
    def test_download_file_success(self, mock_file, mock_get):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
        mock_get.return_value = mock_response

        f.download_file(
            file_url="http://fake-url/file.pdf",
            output_path="/tmp",
            file_name="file.pdf",
            total_size_in_bytes=1024,
        )

        mock_get.assert_called_once_with(
            "http://fake-url/file.pdf", stream=True
        )
        mock_file.assert_called_once_with("/tmp/file.pdf", "wb")

    @mock.patch("ingest_ragflow.dspace_api.files.requests.get")
    def test_get_primary_pdf_bitstream(self, _mock_get):
        bitstreams = [
            {"name": "doc1.pdf", "bundleName": "ORIGINAL", "sizeBytes": 100},
            {"name": "doc2.pdf", "bundleName": "OTHER", "sizeBytes": 200},
        ]
        result = f.get_primary_pdf_bitstream(bitstreams)
        self.assertIsNotNone(result)
        if result is not None:
            self.assertEqual(result["name"], "doc1.pdf")

    def test_get_primary_pdf_bitstream_none(self):
        result = f.get_primary_pdf_bitstream([])
        self.assertIsNone(result)
        result = f.get_primary_pdf_bitstream([{"name": "file.txt"}])
        self.assertIsNone(result)

    @mock.patch("ingest_ragflow.dspace_api.files.get_item_details")
    @mock.patch("ingest_ragflow.dspace_api.files.download_file")
    @mock.patch("os.path.exists", return_value=False)
    @mock.patch("tqdm.tqdm", lambda x, **_kwargs: x)
    def test_retrieve_item_file_success(
        self, _mock_exists, mock_download, mock_get_item
    ):
        mock_get_item.return_value = {
            "bitstreams": [
                {
                    "name": "file.pdf",
                    "retrieveLink": "/retrieve/file.pdf",
                    "bundleName": "ORIGINAL",
                    "sizeBytes": 1234,
                }
            ]
        }

        file_path, item_details = f.retrieve_item_file(
            base_url=self.base_url,
            base_url_rest=self.base_url_rest,
            item_id="item1",
            folder_path="/tmp",
            position=0,
        )

        self.assertIsNotNone(file_path)
        if file_path is not None:
            self.assertIn("file.pdf", file_path)
        self.assertIsNotNone(item_details)
        if item_details is not None:
            self.assertEqual(item_details["bitstreams"][0]["name"], "file.pdf")
        mock_download.assert_called_once()

    @mock.patch("ingest_ragflow.dspace_api.files.get_item_details")
    @mock.patch("tqdm.tqdm", lambda x, **kwargs: x)
    def test_retrieve_item_file_no_bitstreams(self, mock_get_item):
        mock_get_item.return_value = {"bitstreams": []}

        file_path, item_details = f.retrieve_item_file(
            base_url=self.base_url,
            base_url_rest=self.base_url_rest,
            item_id="item1",
            folder_path="/tmp",
            position=0,
        )
        self.assertIsNone(file_path)
        self.assertIsNone(item_details)

    @mock.patch("ingest_ragflow.dspace_api.files.requests.get")
    @mock.patch("ingest_ragflow.dspace_api.files.download_file")
    @mock.patch("tqdm.tqdm", lambda x, **kwargs: x)
    def test_fetch_and_download_files(self, mock_download, mock_get):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "bitstreams": [
                {
                    "retrieveLink": "/file.pdf",
                    "name": "file.pdf",
                    "sizeBytes": 100,
                }
            ]
        }
        mock_get.return_value = mock_response

        f.fetch_and_download_files(
            base_url=self.base_url,
            base_url_rest=self.base_url_rest,
            items_ids=["item1"],
            output_path="/tmp",
        )

        mock_get.assert_called()
        mock_download.assert_called_once()
