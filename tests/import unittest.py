import os
import unittest
from unittest.mock import MagicMock, mock_open, patch

from download_files import save_response


class TestSaveResponse(unittest.TestCase):
    @patch('download_files.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_save_response_json(self, mock_open, mock_get):
        mock_response = MagicMock()
        mock_response.text = '{"key": "value"}'
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        save_response('http://example.com/data.json', 'test.json', 'json')

        mock_get.assert_called_once_with('http://example.com/data.json')

        mock_response.raise_for_status.assert_called_once()

        mock_open.assert_called_once_with('test.json', 'w', encoding='utf-8')

        mock_open().write.assert_called_once_with('{"key": "value"}')

    @patch('download_files.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_save_response_base64(self, mock_open, mock_get):
        mock_response = MagicMock()
        mock_response.text = '"SGVsbG8gd29ybGQ="'
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        save_response('http://example.com/data.b64', 'test.b64', 'base64')

        mock_get.assert_called_once_with('http://example.com/data.b64')
        mock_response.raise_for_status.assert_called_once()
        mock_open.assert_called_once_with('test.b64', 'wb')
        mock_open().write.assert_called_once_with(b'Hello world')

    @patch('download_files.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_save_response_default(self, mock_open, mock_get):
        mock_response = MagicMock()
        mock_response.content = b'binary data'
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        save_response('http://example.com/data.bin', 'test.bin', 'binary')

        mock_get.assert_called_once_with('http://example.com/data.bin')

        mock_response.raise_for_status.assert_called_once()

        mock_open.assert_called_once_with('test.bin', 'wb')

        mock_open().write.assert_called_once_with(b'binary data')


if __name__ == '__main__':
    unittest.main()
