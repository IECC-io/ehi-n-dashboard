#!/usr/bin/env python3
"""
Send Daily 7-Day Forecast Digest to Subscribers

This script runs daily at 6:00 AM IST via GitHub Actions.
It sends a morning summary of the 7-day heat forecast to opted-in subscribers.

Usage:
    python send_daily_digest.py

Environment Variables Required:
    GMAIL_APP_PASSWORD - Gmail app password for sending emails
    GOOGLE_SHEETS_CREDENTIALS - JSON string of service account credentials
    SHEET_ID - Google Sheets document ID
"""

import json
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import pytz

# Google Sheets imports
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# =============================================================================
# CONFIGURATION
# =============================================================================

# Email settings
EMAIL_SENDER = os.environ.get('EMAIL_SENDER', 'eliana101299@gmail.com')
EMAIL_PASSWORD = os.environ.get('GMAIL_APP_PASSWORD')

# Google Sheets
GOOGLE_SHEETS_CREDENTIALS = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
SHEET_ID = os.environ.get('SHEET_ID')

# File paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)  # Parent of scripts/
FORECAST_PATH = os.path.join(ROOT_DIR, 'weather_logs', 'forecast_7day.json')

# Dashboard URL
DASHBOARD_URL = 'https://shram.info'

# Vercel URL for unsubscribe links
VERCEL_URL = os.environ.get('VERCEL_URL', 'shram-alerts.vercel.app')

# Zone colors for email styling
ZONE_COLORS = {
    1: {'bg': '#e3f2fd', 'text': '#1976D2', 'name': 'Cold Stress'},
    2: {'bg': '#e8f5e9', 'text': '#2E7D32', 'name': 'Comfortable'},
    3: {'bg': '#fffde7', 'text': '#F9A825', 'name': 'Moderate'},
    4: {'bg': '#fff3e0', 'text': '#e65100', 'name': 'High'},
    5: {'bg': '#ffebee', 'text': '#d32f2f', 'name': 'Very High'},
    6: {'bg': '#f3e5f5', 'text': '#7B1FA2', 'name': 'Hazardous'}
}


# =============================================================================
# GOOGLE SHEETS FUNCTIONS
# =============================================================================

def get_sheets_client():
    """Initialize Google Sheets client."""
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)


def get_forecast_subscribers():
    """Fetch verified subscribers who opted in for forecasts."""
    if not GOOGLE_SHEETS_CREDENTIALS or not SHEET_ID:
        print("WARNING: Google Sheets credentials not configured.")
        return []

    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SHEET_ID).sheet1
        records = sheet.get_all_records()

        # Filter to verified subscribers who want forecasts
        subscribers = [
            r for r in records
            if r.get('status') == 'verified'
            and r.get('receive_forecasts', '').lower() in ['yes', 'true', '1']
        ]

        print(f"Found {len(subscribers)} subscribers opted in for forecasts")
        return subscribers

    except Exception as e:
        print(f"ERROR fetching subscribers: {e}")
        return []


# =============================================================================
# FORECAST DATA FUNCTIONS
# =============================================================================

def load_forecast_data():
    """Load 7-day forecast data."""
    if not os.path.exists(FORECAST_PATH):
        print(f"WARNING: Forecast file not found at {FORECAST_PATH}")
        return None

    try:
        with open(FORECAST_PATH, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"ERROR loading forecast: {e}")
        return None


def get_district_forecast(forecast_data, district):
    """
    Extract 7-day forecast for a specific district.

    Returns:
        list: [{date, max_zone, max_temp, conditions}, ...]
    """
    if not forecast_data:
        return []

    district_forecast = []

    # Forecast data structure varies - adapt as needed
    # Assuming structure: {days: [{date, points: [{district, zone, temp, ...}]}]}
    days = forecast_data.get('days', [])

    for day in days[:7]:  # Limit to 7 days
        date = day.get('date', '')
        points = day.get('points', [])

        # Find this district's data
        district_data = None
        for point in points:
            if point.get('district') == district:
                district_data = point
                break

        if district_data:
            # Get max zone across all MET levels
            max_zone = 0
            for met in ['met3', 'met4', 'met5', 'met6']:
                for cond in ['shade', 'sun']:
                    try:
                        zone = district_data.get('data', {}).get(met, {}).get(cond, {}).get('zone', 0)
                        max_zone = max(max_zone, zone)
                    except (KeyError, TypeError):
                        pass

            district_forecast.append({
                'date': date,
                'max_zone': max_zone,
                'max_temp': district_data.get('max_temp'),
                'max_rh': district_data.get('max_rh')
            })

    return district_forecast


def get_max_zone_for_districts(forecast_data, districts):
    """
    Get the maximum forecast zone for a list of districts over the next 7 days.

    Returns:
        dict: {district: [{date, max_zone, ...}, ...]}
    """
    result = {}
    for district in districts:
        result[district] = get_district_forecast(forecast_data, district)
    return result


# =============================================================================
# EMAIL FUNCTIONS
# =============================================================================

def format_zone_badge(zone):
    """Create HTML badge for a zone."""
    if zone == 0:
        return '<span style="padding: 2px 8px; border-radius: 4px; background: #f5f5f5; color: #666;">N/A</span>'

    colors = ZONE_COLORS.get(zone, ZONE_COLORS[3])
    return f'<span style="padding: 2px 8px; border-radius: 4px; background: {colors["bg"]}; color: {colors["text"]}; font-weight: 600;">Zone {zone}</span>'


def send_forecast_digest(subscriber, district_forecasts, metadata):
    """
    Send 7-day forecast digest email to a subscriber.

    Args:
        subscriber: Subscriber record from Google Sheets
        district_forecasts: Dict of forecasts by district
        metadata: Forecast metadata
    """
    email = subscriber.get('email')
    name = subscriber.get('name', '')
    token = subscriber.get('verification_token')

    if not email:
        return False

    # Get today's date
    ist = pytz.timezone('Asia/Kolkata')
    today = datetime.now(ist)

    # Build forecast table HTML
    forecast_rows = []

    for district, forecast in district_forecasts.items():
        if not forecast:
            continue

        # Create row for this district
        day_cells = []
        for day in forecast[:7]:
            zone = day.get('max_zone', 0)
            day_cells.append(f'<td style="padding: 8px; text-align: center;">{format_zone_badge(zone)}</td>')

        # Pad if less than 7 days
        while len(day_cells) < 7:
            day_cells.append('<td style="padding: 8px; text-align: center;">-</td>')

        forecast_rows.append(f"""
            <tr>
                <td style="padding: 8px; font-weight: 600; background: #f8f9fa;">{district}</td>
                {''.join(day_cells)}
            </tr>
        """)

    # Get day headers
    day_headers = []
    if district_forecasts:
        first_district_forecast = list(district_forecasts.values())[0]
        for day in first_district_forecast[:7]:
            date_str = day.get('date', '')
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                day_headers.append(f'<th style="padding: 8px; background: #2d637f; color: white;">{date_obj.strftime("%a %d")}</th>')
            except:
                day_headers.append(f'<th style="padding: 8px; background: #2d637f; color: white;">{date_str}</th>')

    # Pad if less than 7 days
    while len(day_headers) < 7:
        day_headers.append('<th style="padding: 8px; background: #2d637f; color: white;">-</th>')

    # Subject line
    subject = f"☀️ SHRAM 7-Day Forecast - {today.strftime('%d %b %Y')}"

    # Build HTML email
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: 'Montserrat', Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 700px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #2d637f 0%, #006D77 100%); color: white; padding: 25px; text-align: center; border-radius: 8px 8px 0 0; }}
            .content {{ background: #f8f9fa; padding: 25px; border-radius: 0 0 8px 8px; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
            th, td {{ border: 1px solid #ddd; }}
            .legend {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 20px 0; }}
            .legend-item {{ display: flex; align-items: center; gap: 5px; font-size: 12px; }}
            .btn {{ display: inline-block; background: #006D77; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; font-weight: 600; }}
            .footer {{ font-size: 12px; color: #666; margin-top: 25px; padding-top: 20px; border-top: 1px solid #ddd; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1 style="margin: 0; font-size: 22px;">☀️ 7-Day Heat Forecast</h1>
                <p style="margin: 10px 0 0 0; opacity: 0.9;">{today.strftime('%A, %d %B %Y')}</p>
            </div>
            <div class="content">
                <p>Good morning{', ' + name if name else ''}!</p>

                <p>Here's your 7-day heat stress forecast for your subscribed districts:</p>

                <table>
                    <thead>
                        <tr>
                            <th style="padding: 8px; background: #2d637f; color: white; text-align: left;">District</th>
                            {''.join(day_headers)}
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(forecast_rows) if forecast_rows else '<tr><td colspan="8" style="padding: 20px; text-align: center;">No forecast data available</td></tr>'}
                    </tbody>
                </table>

                <div class="legend">
                    <span style="font-weight: 600; margin-right: 10px;">Legend:</span>
                    <span class="legend-item">{format_zone_badge(2)} Comfortable</span>
                    <span class="legend-item">{format_zone_badge(3)} Moderate</span>
                    <span class="legend-item">{format_zone_badge(4)} High</span>
                    <span class="legend-item">{format_zone_badge(5)} Very High</span>
                    <span class="legend-item">{format_zone_badge(6)} Hazardous</span>
                </div>

                <p style="text-align: center; margin: 25px 0;">
                    <a href="{DASHBOARD_URL}" class="btn">View Full Forecast on Dashboard</a>
                </p>

                <div class="footer">
                    <p>
                        You're receiving this daily digest because you subscribed to SHRAM forecasts.
                        <br>
                        <a href="https://{VERCEL_URL}/api/unsubscribe?token={token}">Unsubscribe</a> |
                        <a href="{DASHBOARD_URL}">SHRAM Dashboard</a> |
                        <a href="https://iecc.gspp.berkeley.edu/">IECC</a>
                    </p>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

    # Plain text version
    districts_list = '\n'.join([f"  - {d}" for d in district_forecasts.keys()])
    text_body = f"""
    SHRAM 7-Day Heat Forecast
    {today.strftime('%A, %d %B %Y')}

    Good morning{', ' + name if name else ''}!

    Your subscribed districts:
    {districts_list}

    View the full 7-day forecast on the SHRAM Dashboard:
    {DASHBOARD_URL}

    ---
    Unsubscribe: https://{VERCEL_URL}/api/unsubscribe?token={token}
    """

    # Send email
    try:
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

        print(f"  ✓ Digest sent to {email}")
        return True

    except Exception as e:
        print(f"  ✗ Failed to send to {email}: {e}")
        return False


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    """Main daily digest logic."""
    print("=" * 60)
    print("SHRAM Daily Forecast Digest")
    print("=" * 60)

    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    print(f"Running at: {now.strftime('%Y-%m-%d %H:%M:%S IST')}")

    # Check credentials
    if not EMAIL_PASSWORD:
        print("ERROR: GMAIL_APP_PASSWORD not set. Cannot send emails.")
        return

    # Load forecast data
    print("\n[1/3] Loading 7-day forecast data...")
    forecast_data = load_forecast_data()

    if not forecast_data:
        print("WARNING: No forecast data available. Sending basic digest.")

    # Get subscribers who want forecasts
    print("\n[2/3] Fetching subscribers opted in for forecasts...")
    subscribers = get_forecast_subscribers()

    if not subscribers:
        print("No subscribers opted in for daily forecasts.")
        return

    # Send digest to each subscriber
    print(f"\n[3/3] Sending daily digest to {len(subscribers)} subscribers...")
    digests_sent = 0
    digests_failed = 0

    for subscriber in subscribers:
        # Parse subscriber's districts
        sub_districts_str = subscriber.get('districts', '')
        sub_districts = [d.strip() for d in sub_districts_str.split(',') if d.strip()]

        if not sub_districts:
            print(f"  Skipping {subscriber.get('email')} - no districts configured")
            continue

        # Get forecast for their districts
        district_forecasts = get_max_zone_for_districts(forecast_data, sub_districts)

        # Send digest
        success = send_forecast_digest(
            subscriber,
            district_forecasts,
            forecast_data.get('metadata', {}) if forecast_data else {}
        )

        if success:
            digests_sent += 1
        else:
            digests_failed += 1

    # Summary
    print("\n" + "=" * 60)
    print("DIGEST SUMMARY")
    print("=" * 60)
    print(f"  Subscribers with forecast opt-in: {len(subscribers)}")
    print(f"  Digests sent: {digests_sent}")
    if digests_failed:
        print(f"  Digests failed: {digests_failed}")
    print("=" * 60)


if __name__ == '__main__':
    main()
