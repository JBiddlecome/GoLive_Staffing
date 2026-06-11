import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Ensure the root directory is in python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Remove RequireLoginMiddleware so we can hit the endpoints without a session
from app import app
from starlette.middleware.base import BaseHTTPMiddleware
for middleware in list(app.user_middleware):
    if middleware.cls.__name__ == 'RequireLoginMiddleware':
        app.user_middleware.remove(middleware)
app.middleware_stack = app.build_middleware_stack()

from fastapi.testclient import TestClient
client = TestClient(app)

class TestEmailExplorerNewRoutes(unittest.TestCase):
    @patch('apps.email_explorer.views._get_access_token')
    @patch('apps.email_explorer.views.requests.get')
    def test_get_message_detail_success(self, mock_get, mock_token):
        mock_token.return_value = "mock_token"
        
        # Mock Response for message details
        mock_msg_response = MagicMock()
        mock_msg_response.status_code = 200
        mock_msg_response.json.return_value = {
            "id": "msg123",
            "subject": "Hello World",
            "receivedDateTime": "2026-06-11T12:00:00Z",
            "sender": {"emailAddress": {"name": "Sender Name", "address": "sender@test.com"}},
            "toRecipients": [{"emailAddress": {"name": "Recipient Name", "address": "to@test.com"}}],
            "ccRecipients": [],
            "body": {"contentType": "html", "content": "<div>Hello World Body</div>"},
            "hasAttachments": True
        }
        
        # Mock Response for attachments
        mock_att_response = MagicMock()
        mock_att_response.status_code = 200
        mock_att_response.json.return_value = {
            "value": [
                {
                    "id": "att456",
                    "name": "invoice.pdf",
                    "contentType": "application/pdf",
                    "size": 1024,
                    "isInline": False
                }
            ]
        }
        
        # requests.get will be called twice: message details then attachments list
        mock_get.side_effect = [mock_msg_response, mock_att_response]
        
        response = client.get('/email-explorer/message/sender@test.com/msg123')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "success")
        self.assertEqual(data["data"]["subject"], "Hello World")
        self.assertEqual(len(data["data"]["attachments"]), 1)
        self.assertEqual(data["data"]["attachments"][0]["name"], "invoice.pdf")

    @patch('apps.email_explorer.views._get_access_token')
    @patch('apps.email_explorer.views.requests.get')
    def test_download_attachment_success(self, mock_get, mock_token):
        mock_token.return_value = "mock_token"
        
        # Mock Response for attachment fetch
        mock_att_response = MagicMock()
        mock_att_response.status_code = 200
        import base64
        test_bytes = b"PDF-dummy-content"
        base64_bytes = base64.b64encode(test_bytes).decode('utf-8')
        mock_att_response.json.return_value = {
            "name": "invoice.pdf",
            "contentType": "application/pdf",
            "contentBytes": base64_bytes
        }
        mock_get.return_value = mock_att_response
        
        response = client.get('/email-explorer/message/sender@test.com/msg123/attachment/att456')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, test_bytes)
        self.assertEqual(response.headers["content-type"], "application/pdf")
        self.assertIn('attachment; filename="invoice.pdf"', response.headers["content-disposition"])

if __name__ == '__main__':
    unittest.main()
