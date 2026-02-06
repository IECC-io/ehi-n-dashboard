"""
Vercel Serverless Function: Handle unsubscribe requests

Endpoint: GET /api/unsubscribe?token=<uuid>

Actions:
1. Look up token in Google Sheets
2. Mark as unsubscribed (or delete row)
3. Redirect to confirmation page
"""

from http.server import BaseHTTPRequestHandler
import json
import os
from datetime import datetime
from urllib.parse import parse_qs, urlparse
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# Configuration from environment variables
GOOGLE_SHEETS_CREDENTIALS = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
SHEET_ID = os.environ.get('SHEET_ID')
DASHBOARD_URL = 'https://shram.info'


def get_sheets_client():
    """Initialize Google Sheets client."""
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)


def find_subscriber_by_token(sheet, token):
    """Find subscriber row by verification token."""
    try:
        records = sheet.get_all_records()
        for i, record in enumerate(records):
            if record.get('verification_token') == token:
                return i + 2, record  # +2 for header row and 1-indexing
        return None, None
    except Exception as e:
        print(f"Error finding subscriber: {e}")
        return None, None


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Handle unsubscribe request."""
        try:
            # Parse query parameters
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)
            token = query_params.get('token', [None])[0]

            if not token:
                # Redirect to error page
                self.send_response(302)
                self.send_header('Location', f'{DASHBOARD_URL}/unsubscribe-error.html?reason=missing_token')
                self.end_headers()
                return

            # Connect to Google Sheets
            client = get_sheets_client()
            sheet = client.open_by_key(SHEET_ID).sheet1

            # Find subscriber by token
            row_num, subscriber = find_subscriber_by_token(sheet, token)

            if not subscriber:
                # Token not found - might already be unsubscribed
                self.send_response(302)
                self.send_header('Location', f'{DASHBOARD_URL}/unsubscribe-success.html?already=true')
                self.end_headers()
                return

            # Check if already unsubscribed
            if subscriber.get('status') == 'unsubscribed':
                self.send_response(302)
                self.send_header('Location', f'{DASHBOARD_URL}/unsubscribe-success.html?already=true')
                self.end_headers()
                return

            # Mark as unsubscribed (keeping record for audit trail)
            # Alternatively, could delete the row with: sheet.delete_rows(row_num)
            sheet.update_cell(row_num, 6, 'unsubscribed')  # status column

            # Redirect to success page
            self.send_response(302)
            self.send_header('Location', f'{DASHBOARD_URL}/unsubscribe-success.html')
            self.end_headers()

        except Exception as e:
            print(f"Unsubscribe error: {e}")
            self.send_response(302)
            self.send_header('Location', f'{DASHBOARD_URL}/unsubscribe-error.html?reason=error')
            self.end_headers()
