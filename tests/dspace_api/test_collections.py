from unittest import TestCase, mock

import pandas as pd

from ingest_ragflow.dspace_api import collections as col


class TestCollections(TestCase):
    def setUp(self) -> None:
        self.base_url = "http://test-ri.com"
        self.base_url_rest = "http://base-url-rest"

    @mock.patch("ingest_ragflow.dspace_api.collections.requests.get")
    def test_get_items_from_collection_success(self, mock_get):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"uuid": "item1", "name": "Item One"},
            {"uuid": "item2", "name": "Item Two"},
        ]
        mock_get.return_value = mock_response

        result = col.get_items_from_collection(
            collection_id="123", base_url_rest=self.base_url_rest
        )
        self.assertEqual(result, ["item1", "item2"])

    @mock.patch("ingest_ragflow.dspace_api.collections.requests.get")
    def test_get_items_from_collection_empty(self, mock_get):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        result = col.get_items_from_collection("123", self.base_url_rest)
        self.assertEqual(result, [])

    @mock.patch("builtins.input", return_value="1")
    def test_select_collection(self, _mock_input):
        collections_ids = ["col1", "col2", "col3"]
        result = col.select_collection(collections_ids)
        self.assertEqual(result, "col2")

    @mock.patch("ingest_ragflow.dspace_api.collections.requests.get")
    def test_get_collections_success(self, mock_get):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"uuid": "col1", "name": "Collection 1"},
            {"uuid": "col2", "name": "Collection 2"},
        ]
        mock_get.return_value = mock_response

        result = col.get_collections(self.base_url_rest)
        self.assertEqual(result, ["col1", "col2"])

    @mock.patch(
        "ingest_ragflow.dspace_api.collections.get_items_from_collection"
    )
    @mock.patch("ingest_ragflow.dspace_api.collections.requests.get")
    def test_get_collection_stats(self, mock_get, mock_get_items):
        mock_get_items.return_value = ["item1"]
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"bitstreams": [{"sizeBytes": 500}]}
        mock_get.return_value = mock_response

        item_count, total_size = col.get_collection_stats(
            self.base_url_rest, "col1"
        )
        self.assertEqual(item_count, 1)
        self.assertEqual(total_size, 500)

    @mock.patch("ingest_ragflow.dspace_api.collections.get_collections")
    @mock.patch("ingest_ragflow.dspace_api.collections.get_collection_stats")
    @mock.patch("ingest_ragflow.dspace_api.collections.requests.get")
    @mock.patch("tqdm.tqdm", lambda x, **_kwargs: x)  # skip progress bar
    def test_generate_collection_stats(
        self, mock_get, mock_get_stats, mock_get_cols
    ):
        mock_get_cols.return_value = ["col1", "col2"]
        mock_get_stats.side_effect = [(1, 500), (2, 1000)]

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"name": "Fake Collection"}
        mock_get.return_value = mock_response

        df = col.generate_collection_stats(self.base_url_rest)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape[0], 3)  # 2 collections + totals row
        self.assertEqual(df["Number of Documents"].iloc[-1], 3)  # sum of items
        self.assertEqual(
            df["Total Size (Bytes)"].iloc[-1], 1500
        )  # sum of sizes
