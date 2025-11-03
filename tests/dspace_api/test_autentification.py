from unittest import TestCase, mock

import requests

from ingest_ragflow.dspace_api import authentification as auth


class TestAuth(TestCase):
    def setUp(self) -> None:
        self.base_url = "http://test-ri.com"
        self.base_url_rest = "http://test-ri-rest.com"

    @mock.patch("ingest_ragflow.dspace_api.authentification.Session.post")
    def test_authenticate_user_success(self, mock_post):
        mock_response = mock.Mock()
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        session = requests.Session()
        result = auth.authenticate_user(
            session=session,
            email="test@example.com",
            password="password123",
            base_url_rest=self.base_url_rest,
        )

        self.assertTrue(result)
        mock_post.assert_called_once()

    @mock.patch("ingest_ragflow.dspace_api.authentification.Session.post")
    def test_authenticate_user_failure(self, mock_post):
        mock_post.side_effect = requests.exceptions.RequestException(
            "Invalid credentials"
        )

        session = requests.Session()
        result = auth.authenticate_user(
            session=session,
            email="wrong@example.com",
            password="badpass",
            base_url_rest=self.base_url_rest,
        )

        self.assertFalse(result)
        mock_post.assert_called_once()
