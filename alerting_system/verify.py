"""
Vercel Serverless Function: Handle email verification

Endpoint: GET /api/verify?token=<uuid>

Actions:
1. Look up token in Google Sheets
2. Update status to "verified"
3. Redirect to success page
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
        """Handle verification request."""
        try:
            # Parse query parameters
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)
            token = query_params.get('token', [None])[0]

            if not token:
                # Redirect to error page
                self.send_response(302)
                self.send_header('Location', f'{DASHBOARD_URL}/verify-error.html?reason=missing_token')
                self.end_headers()
                return

            # Connect to Google Sheets
            client = get_sheets_client()
            sheet = client.open_by_key(SHEET_ID).sheet1

            # Find subscriber by token
            row_num, subscriber = find_subscriber_by_token(sheet, token)

            if not subscriber:
                # Token not found
                self.send_response(302)
                self.send_header('Location', f'{DASHBOARD_URL}/verify-error.html?reason=invalid_token')
                self.end_headers()
                return

            # Check if already verified
            if subscriber.get('status') == 'verified':
                # Already verified - redirect to success anyway
                self.send_response(302)
                self.send_header('Location', f'{DASHBOARD_URL}/verify-success.html?already=true')
                self.end_headers()
                return

            # Update status to verified
            # Columns: email(A), name(B), districts(C), receive_forecasts(D),
            #          verification_token(E), status(F), subscribed_at(G), verified_at(H), last_alert_sent(I)
            now = datetime.utcnow().isoformat() + 'Z'

            # Update status (column F) and verified_at (column H)
            sheet.update_cell(row_num, 6, 'verified')  # status
            sheet.update_cell(row_num, 8, now)  # verified_at

            # Redirect to success page
            self.send_response(302)
            self.send_header('Location', f'{DASHBOARD_URL}/verify-success.html')
            self.end_headers()

        except Exception as e:
            print(f"Verification error: {e}")
            self.send_response(302)
            self.send_header('Location', f'{DASHBOARD_URL}/verify-error.html?reason=error')
            self.end_headers()
