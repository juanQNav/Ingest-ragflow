from unittest import TestCase, mock

import pandas as pd

from ingest_ragflow.dspace_api import items as it


class TestItems(TestCase):
    def setUp(self) -> None:
        self.base_url = "http://test-ri.com"
        self.base_url_rest = "http://base-url-rest"

    @mock.patch("ingest_ragflow.dspace_api.items.requests.get")
    def test_get_items_success(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.side_effect = [
            [{"uuid": "id1", "name": "item1"}],
            [],
        ]

        result = it.get_items(base_url_rest=self.base_url_rest, verbose=False)
        self.assertIsNotNone(result)
        if result is not None:
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["uuid"], "id1")
        mock_get.assert_called()

    def test_get_items_ids(self):
        items = [{"uuid": "id1"}, {"uuid": "id2"}, {"uuid": "id1"}]
        ids = it.get_items_ids(items)
        self.assertEqual(ids, ["id1", "id2"])

    @mock.patch("ingest_ragflow.dspace_api.items.requests.get")
    def test_get_item_metadata_success(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [
            {"key": "title", "value": "Document 1"},
            {"key": "authors", "value": ["Alice", "Bob"]},
        ]
        metadata = it.get_item_metadata(self.base_url_rest, "item1")
        self.assertIsNotNone(metadata)
        if metadata is not None:
            self.assertEqual(metadata["title"], "Document 1")
            self.assertEqual(metadata["authors"], ["Alice", "Bob"])

    @mock.patch("ingest_ragflow.dspace_api.items.get_item_metadata")
    @mock.patch("ingest_ragflow.dspace_api.items.requests.get")
    def test_get_item_details_success(self, mock_get, mock_metadata):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "uuid": "id1",
            "name": "Item 1",
            "handle": "12345/1",
            "inArchive": True,
            "discoverable": True,
            "withdrawn": False,
            "lastModified": "2025-01-01",
            "bitstreams": [{"name": "file.pdf"}],
        }
        mock_metadata.return_value = {"title": "Document 1"}

        details = it.get_item_details(self.base_url_rest, "id1")
        self.assertIsNotNone(details)
        if details is not None:
            self.assertEqual(details["uuid"], "id1")
            self.assertIn("metadata", details)
            self.assertEqual(details["metadata"]["title"], "Document 1")

    @mock.patch("ingest_ragflow.dspace_api.items.requests.get")
    def test_get_item_stats(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "bitstreams": [{"name": "file.pdf", "sizeBytes": 1234}]
        }
        item = {"uuid": "id1", "name": "Item 1"}
        result = it.get_item_stats(self.base_url_rest, item)
        self.assertEqual(result, ("id1", "Item 1", "file.pdf", 1234))

    @mock.patch("ingest_ragflow.dspace_api.items.get_items")
    @mock.patch("ingest_ragflow.dspace_api.items.get_item_stats")
    @mock.patch("tqdm.tqdm", lambda x, **kwargs: x)
    def test_generate_item_stats(self, mock_get_stats, mock_get_items):
        mock_get_items.return_value = [{"uuid": "id1", "name": "Item 1"}]
        mock_get_stats.return_value = ("id1", "Item 1", "file.pdf", 1234)

        df = it.generate_item_stats(self.base_url_rest)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.iloc[0]["uuid"], "id1")
        self.assertEqual(df.iloc[0]["size_Bytes"], 1234)
        self.assertEqual(df.iloc[-1]["uuid"], "Total documents")
