from http.server import BaseHTTPRequestHandler
import json
import os
import uuid
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

EMAIL_SENDER = os.environ.get('EMAIL_SENDER')
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD')
GOOGLE_SHEETS_CREDENTIALS = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
SHEET_ID = os.environ.get('SHEET_ID')
VERCEL_BASE_URL = 'shram-alerts.vercel.app'
DASHBOARD_URL = 'https://shram.info'


def get_sheets_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)


def log_subscriber_activity(client, action, email, details=None):
    """
    Log subscriber activity to a separate 'Activity Log' sheet.

    Args:
        client: gspread client
        action: 'subscribed', 'verified', 'unsubscribed', 'resent_verification', 'preferences_changed'
        email: subscriber email
        details: optional dict with additional info
    """
    try:
        spreadsheet = client.open_by_key(SHEET_ID)

        # Get or create Activity Log sheet
        try:
            log_sheet = spreadsheet.worksheet('Activity Log')
        except gspread.exceptions.WorksheetNotFound:
            log_sheet = spreadsheet.add_worksheet(title='Activity Log', rows=1000, cols=10)
            log_sheet.append_row(['timestamp', 'action', 'email', 'details', 'ip_address'])

        now = datetime.utcnow().isoformat() + 'Z'
        details_str = json.dumps(details) if details else ''

        log_sheet.append_row([now, action, email, details_str, ''])
    except Exception as e:
        print(f"Warning: Could not log activity: {e}")


def check_existing_subscriber(sheet, email):
    try:
        records = sheet.get_all_records()
        for i, record in enumerate(records):
            if record.get('email', '').lower() == email.lower():
                return i + 2, record
        return None, None
    except:
        return None, None


def send_verification_email(email, name, token):
    verify_url = f"https://{VERCEL_BASE_URL}/api/verify?token={token}"
    subject = "Verify your SHRAM Heat Alert Subscription"

    html_body = f"""<!DOCTYPE html>
<html><head><style>
body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
.container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
.header {{ background: #006D77; color: white; padding: 30px; text-align: center; }}
.content {{ background: #f8f9fa; padding: 30px; }}
.btn {{ display: inline-block; background: #006D77; color: white; padding: 14px 28px; text-decoration: none; border-radius: 6px; }}
</style></head><body>
<div class="container">
<div class="header"><h1>SHRAM Heat Alerts</h1></div>
<div class="content">
<h2>Welcome{', ' + name if name else ''}!</h2>
<p>Please verify your email:</p>
<p><a href="{verify_url}" class="btn" style="color: #ffffff !important; background: #006D77; padding: 14px 28px; text-decoration: none; border-radius: 6px; display: inline-block;">Verify Email Address</a></p>
<p style="font-size: 13px;">Or copy: {verify_url}</p>
</div></div></body></html>"""

    text_body = f"Welcome to SHRAM Heat Alerts! Verify your email: {verify_url}"

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = EMAIL_SENDER
    msg['To'] = email
    msg.attach(MIMEText(text_body, 'plain'))
    msg.attach(MIMEText(html_body, 'html'))

    with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
        smtp.starttls()
        smtp.login(EMAIL_SENDER, EMAIL_PASSWORD)
        smtp.send_message(msg)


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_POST(self):
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length)
            data = json.loads(body)

            email = data.get('email', '').strip().lower()
            name = data.get('name', '').strip()
            phone = data.get('phone', '').strip()  # For future SMS alerts
            districts = data.get('districts', [])
            met_levels = data.get('met_levels', [6])  # Default to MET 6
            alert_zones = data.get('alert_zones', [6])  # Default to Zone 6
            sun_shade = data.get('sun_shade', 'shade')  # Default to shade
            receive_forecasts = data.get('receive_forecasts', True)
            receive_sms = data.get('receive_sms', False)  # For future SMS alerts

            if not email or '@' not in email:
                self.send_response(400)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': 'Valid email required'}).encode())
                return

            if not districts:
                self.send_response(400)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'success': False, 'error': 'Select at least one district'}).encode())
                return

            client = get_sheets_client()
            sheet = client.open_by_key(SHEET_ID).sheet1

            row_num, existing = check_existing_subscriber(sheet, email)
            if existing:
                if existing.get('status') == 'verified':
                    self.send_response(400)
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({'success': False, 'error': 'Already subscribed'}).encode())
                    return
                else:
                    token = existing.get('verification_token')
                    send_verification_email(email, name, token)
                    log_subscriber_activity(client, 'resent_verification', email)
                    self.send_response(200)
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({'success': True, 'message': 'Verification email re-sent'}).encode())
                    return

            token = str(uuid.uuid4())
            now = datetime.utcnow().isoformat() + 'Z'
            districts_str = ','.join(districts) if isinstance(districts, list) else districts
            met_levels_str = ','.join(str(m) for m in met_levels) if isinstance(met_levels, list) else str(met_levels)
            alert_zones_str = ','.join(str(z) for z in alert_zones) if isinstance(alert_zones, list) else str(alert_zones)

            # Columns: email, name, phone, districts, met_levels, alert_zones, sun_shade, receive_forecasts, receive_sms, verification_token, status, subscribed_at, verified_at, last_alert_sent
            sheet.append_row([
                email, name, phone, districts_str, met_levels_str, alert_zones_str, sun_shade,
                'yes' if receive_forecasts else 'no', 'yes' if receive_sms else 'no',
                token, 'pending', now, '', ''
            ])

            # Log subscription activity
            log_subscriber_activity(client, 'subscribed', email, {
                'name': name,
                'phone': phone,
                'districts': districts,
                'met_levels': met_levels,
                'alert_zones': alert_zones,
                'sun_shade': sun_shade,
                'receive_sms': receive_sms
            })
            send_verification_email(email, name, token)

            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'success': True, 'message': 'Verification email sent!'}).encode())

        except Exception as e:
            print(f"Error: {e}")
            self.send_response(500)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'success': False, 'error': 'Server error'}).encode())
