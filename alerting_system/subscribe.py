"""
Vercel Serverless Function: Handle new subscription requests

Endpoint: POST /api/subscribe
Body: { email, name, districts, receive_forecasts }

Actions:
1. Validate input
2. Generate verification token
3. Add to Google Sheets (status: pending)
4. Send verification email
5. Return success/error
"""

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


# Configuration from environment variables
EMAIL_SENDER = os.environ.get('EMAIL_SENDER')
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD')
GOOGLE_SHEETS_CREDENTIALS = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
SHEET_ID = os.environ.get('SHEET_ID')

# Base URL for verification links (update after Vercel deployment)
VERCEL_BASE_URL = os.environ.get('VERCEL_URL', 'shram-alerts.vercel.app')
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


def check_existing_subscriber(sheet, email):
    """Check if email already exists in sheet."""
    try:
        records = sheet.get_all_records()
        for i, record in enumerate(records):
            if record.get('email', '').lower() == email.lower():
                return i + 2, record  # +2 for header row and 1-indexing
        return None, None
    except Exception:
        return None, None


def send_verification_email(email, name, token):
    """Send verification email to subscriber."""
    verify_url = f"https://{VERCEL_BASE_URL}/api/verify?token={token}"

    subject = "Verify your SHRAM Heat Alert Subscription"

    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: 'Montserrat', Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #2d637f 0%, #006D77 100%); color: white; padding: 30px; text-align: center; border-radius: 8px 8px 0 0; }}
            .content {{ background: #f8f9fa; padding: 30px; border-radius: 0 0 8px 8px; }}
            .btn {{ display: inline-block; background: #006D77; color: white; padding: 14px 28px; text-decoration: none; border-radius: 6px; font-weight: 600; margin: 20px 0; }}
            .btn:hover {{ background: #005a63; }}
            .footer {{ font-size: 12px; color: #666; margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1 style="margin: 0;">SHRAM Heat Alerts</h1>
                <p style="margin: 10px 0 0 0; opacity: 0.9;">India Energy & Climate Center</p>
            </div>
            <div class="content">
                <h2>Welcome{', ' + name if name else ''}!</h2>
                <p>Thank you for subscribing to SHRAM heat stress alerts. You'll receive:</p>
                <ul>
                    <li><strong>Instant alerts</strong> when Zone 6 (hazardous) heat stress is detected in your selected districts</li>
                    <li><strong>7-day forecast digest</strong> (if opted in) every morning</li>
                </ul>
                <p>Please verify your email address to activate your subscription:</p>
                <p style="text-align: center;">
                    <a href="{verify_url}" class="btn">Verify Email Address</a>
                </p>
                <p style="font-size: 13px; color: #666;">Or copy this link: {verify_url}</p>
                <p style="font-size: 13px; color: #666;">This link expires in 7 days.</p>
                <div class="footer">
                    <p>If you didn't subscribe to SHRAM alerts, you can safely ignore this email.</p>
                    <p>
                        <a href="{DASHBOARD_URL}">SHRAM Dashboard</a> |
                        <a href="https://iecc.gspp.berkeley.edu/">India Energy & Climate Center</a>
                    </p>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

    text_body = f"""
    Welcome to SHRAM Heat Alerts!

    Thank you for subscribing. Please verify your email to activate your subscription:
    {verify_url}

    You'll receive instant alerts when Zone 6 heat stress is detected in your selected districts.

    If you didn't subscribe, you can ignore this email.
    """

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
        """Handle CORS preflight requests."""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_POST(self):
        """Handle subscription request."""
        # CORS headers
        self.send_header('Access-Control-Allow-Origin', '*')

        try:
            # Parse request body
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length)
            data = json.loads(body)

            email = data.get('email', '').strip().lower()
            name = data.get('name', '').strip()
            districts = data.get('districts', [])
            receive_forecasts = data.get('receive_forecasts', True)

            # Validate email
            if not email or '@' not in email:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'success': False,
                    'error': 'Valid email address is required'
                }).encode())
                return

            # Validate districts
            if not districts or len(districts) == 0:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'success': False,
                    'error': 'At least one district must be selected'
                }).encode())
                return

            # Connect to Google Sheets
            client = get_sheets_client()
            sheet = client.open_by_key(SHEET_ID).sheet1

            # Check if already subscribed
            row_num, existing = check_existing_subscriber(sheet, email)
            if existing:
                if existing.get('status') == 'verified':
                    self.send_response(400)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        'success': False,
                        'error': 'This email is already subscribed. Check your inbox for alerts.'
                    }).encode())
                    return
                else:
                    # Re-send verification for pending subscription
                    token = existing.get('verification_token')
                    send_verification_email(email, name, token)
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        'success': True,
                        'message': 'Verification email re-sent. Please check your inbox.'
                    }).encode())
                    return

            # Generate verification token
            token = str(uuid.uuid4())

            # Prepare row data
            now = datetime.utcnow().isoformat() + 'Z'
            districts_str = ','.join(districts) if isinstance(districts, list) else districts

            row_data = [
                email,
                name,
                districts_str,
                'yes' if receive_forecasts else 'no',
                token,
                'pending',
                now,
                '',  # verified_at (empty until verified)
                ''   # last_alert_sent (empty)
            ]

            # Add to Google Sheets
            sheet.append_row(row_data)

            # Send verification email
            send_verification_email(email, name, token)

            # Success response
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                'success': True,
                'message': 'Verification email sent! Please check your inbox and click the verification link.'
            }).encode())

        except json.JSONDecodeError:
            self.send_response(400)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                'success': False,
                'error': 'Invalid JSON in request body'
            }).encode())

        except Exception as e:
            print(f"Subscription error: {e}")
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                'success': False,
                'error': 'Failed to process subscription. Please try again.'
            }).encode())
