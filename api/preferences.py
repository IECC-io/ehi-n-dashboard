from http.server import BaseHTTPRequestHandler
import json
import os
from datetime import datetime
from urllib.parse import parse_qs, urlparse
import gspread
from oauth2client.service_account import ServiceAccountCredentials

GOOGLE_SHEETS_CREDENTIALS = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
SHEET_ID = os.environ.get('SHEET_ID')
DASHBOARD_URL = 'https://shram.info'


def get_sheets_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)


def log_subscriber_activity(client, action, email, details=None):
    """Log subscriber activity to Activity Log sheet."""
    try:
        spreadsheet = client.open_by_key(SHEET_ID)
        try:
            log_sheet = spreadsheet.worksheet('Activity Log')
        except:
            log_sheet = spreadsheet.add_worksheet(title='Activity Log', rows=1000, cols=10)
            log_sheet.append_row(['timestamp', 'action', 'email', 'details', 'ip_address'])

        now = datetime.utcnow().isoformat() + 'Z'
        details_str = json.dumps(details) if details else ''
        log_sheet.append_row([now, action, email, details_str, ''])
    except Exception as e:
        print(f"Warning: Could not log activity: {e}")


def find_subscriber_by_token(sheet, token):
    """Find subscriber row by verification token."""
    try:
        records = sheet.get_all_records()
        for i, record in enumerate(records):
            if record.get('verification_token') == token:
                return i + 2, record  # +2 for header row and 0-indexing
        return None, None
    except:
        return None, None


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        """Handle CORS preflight."""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        """
        GET /api/preferences?token=xxx
        Returns current preferences for the subscriber.
        """
        try:
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            token = params.get('token', [None])[0]

            if not token:
                self.send_response(400)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': 'Missing token'}).encode())
                return

            client = get_sheets_client()
            sheet = client.open_by_key(SHEET_ID).sheet1
            row_num, subscriber = find_subscriber_by_token(sheet, token)

            if not subscriber:
                self.send_response(404)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': 'Subscriber not found'}).encode())
                return

            if subscriber.get('status') == 'unsubscribed':
                self.send_response(410)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': 'Subscription inactive'}).encode())
                return

            # Return current preferences (exclude sensitive fields)
            preferences = {
                'email': subscriber.get('email', ''),
                'name': subscriber.get('name', ''),
                'phone': subscriber.get('phone', ''),
                'districts': subscriber.get('districts', ''),
                'met_levels': subscriber.get('met_levels', '6'),
                'alert_zones': subscriber.get('alert_zones', '6'),
                'sun_shade': subscriber.get('sun_shade', 'shade'),
                'receive_forecasts': subscriber.get('receive_forecasts', '') in ['TRUE', 'true', True, 1, '1'],
                'receive_sms': subscriber.get('receive_sms', '') in ['TRUE', 'true', True, 1, '1'],
                'status': subscriber.get('status', '')
            }

            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'success': True, 'preferences': preferences}).encode())

        except Exception as e:
            print(f"Error: {e}")
            self.send_response(500)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'success': False, 'error': 'Server error'}).encode())

    def do_POST(self):
        """
        POST /api/preferences?token=xxx
        Updates subscriber preferences.

        Body: {
            "name": "...",
            "phone": "...",
            "districts": "District1, District2",
            "met_levels": "5,6",
            "alert_zones": "5,6",
            "sun_shade": "shade|sun|both",
            "receive_forecasts": true|false,
            "receive_sms": true|false
        }
        """
        try:
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            token = params.get('token', [None])[0]

            if not token:
                self.send_response(400)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': 'Missing token'}).encode())
                return

            # Parse request body
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            data = json.loads(body) if body else {}

            client = get_sheets_client()
            sheet = client.open_by_key(SHEET_ID).sheet1
            row_num, subscriber = find_subscriber_by_token(sheet, token)

            if not subscriber:
                self.send_response(404)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': 'Subscriber not found'}).encode())
                return

            if subscriber.get('status') == 'unsubscribed':
                self.send_response(410)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': 'Subscription inactive'}).encode())
                return

            # Column mapping:
            # 1=email, 2=name, 3=phone, 4=districts, 5=met_levels, 6=alert_zones,
            # 7=sun_shade, 8=receive_forecasts, 9=receive_sms, 10=verification_token,
            # 11=status, 12=subscribed_at, 13=verified_at, 14=last_alert_sent

            updates_made = []

            # Update allowed fields
            if 'name' in data:
                sheet.update_cell(row_num, 2, data['name'])
                updates_made.append('name')

            if 'phone' in data:
                sheet.update_cell(row_num, 3, data['phone'])
                updates_made.append('phone')

            if 'districts' in data:
                # Validate districts is a non-empty string
                districts = data['districts'].strip()
                if districts:
                    sheet.update_cell(row_num, 4, districts)
                    updates_made.append('districts')

            if 'met_levels' in data:
                # Validate MET levels (should be comma-separated numbers 3-6)
                met_str = str(data['met_levels']).strip()
                try:
                    met_vals = [int(m.strip()) for m in met_str.split(',') if m.strip()]
                    valid_mets = [m for m in met_vals if 3 <= m <= 6]
                    if valid_mets:
                        sheet.update_cell(row_num, 5, ','.join(map(str, valid_mets)))
                        updates_made.append('met_levels')
                except ValueError:
                    pass  # Invalid format, skip

            if 'alert_zones' in data:
                # Validate alert zones (should be comma-separated numbers 4-6)
                zones_str = str(data['alert_zones']).strip()
                try:
                    zone_vals = [int(z.strip()) for z in zones_str.split(',') if z.strip()]
                    valid_zones = [z for z in zone_vals if 4 <= z <= 6]
                    if valid_zones:
                        sheet.update_cell(row_num, 6, ','.join(map(str, valid_zones)))
                        updates_made.append('alert_zones')
                except ValueError:
                    pass

            if 'sun_shade' in data:
                sun_shade = data['sun_shade']
                if sun_shade in ['shade', 'sun', 'both']:
                    sheet.update_cell(row_num, 7, sun_shade)
                    updates_made.append('sun_shade')

            if 'receive_forecasts' in data:
                value = 'TRUE' if data['receive_forecasts'] else 'FALSE'
                sheet.update_cell(row_num, 8, value)
                updates_made.append('receive_forecasts')

            if 'receive_sms' in data:
                value = 'TRUE' if data['receive_sms'] else 'FALSE'
                sheet.update_cell(row_num, 9, value)
                updates_made.append('receive_sms')

            # Log the update
            log_subscriber_activity(client, 'preferences_updated', subscriber.get('email', ''), {
                'fields_updated': updates_made
            })

            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                'success': True,
                'message': 'Preferences updated',
                'updated_fields': updates_made
            }).encode())

        except json.JSONDecodeError:
            self.send_response(400)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'success': False, 'error': 'Invalid JSON'}).encode())

        except Exception as e:
            print(f"Error: {e}")
            self.send_response(500)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'success': False, 'error': 'Server error'}).encode())
