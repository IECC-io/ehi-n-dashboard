#!/usr/bin/env python3
"""
Send Zone 6 Heat Stress Alerts to Verified Subscribers

This script runs after generate_grid_data_openmeteo.py in GitHub Actions.
It checks for NEW Zone 6 conditions and sends instant alerts to subscribers.

Usage:
    python send_alerts.py

Environment Variables Required:
    GMAIL_APP_PASSWORD - Gmail app password for sending emails
    GOOGLE_SHEETS_CREDENTIALS - JSON string of service account credentials
    SHEET_ID - Google Sheets document ID (optional, defaults to env var or config)
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

# Email settings (use IECC account)
EMAIL_SENDER = os.environ.get('EMAIL_SENDER', 'eliana101299@gmail.com')
EMAIL_PASSWORD = os.environ.get('GMAIL_APP_PASSWORD')

# Google Sheets
GOOGLE_SHEETS_CREDENTIALS = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
SHEET_ID = os.environ.get('SHEET_ID')

# File paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)  # Parent of scripts/
GRID_DATA_PATH = os.path.join(ROOT_DIR, 'grid_data.json')
ALERT_STATE_PATH = os.path.join(ROOT_DIR, 'weather_logs', 'alert_state.json')

# Dashboard URL
DASHBOARD_URL = 'https://shram.info'

# Vercel URL for unsubscribe links
VERCEL_URL = os.environ.get('VERCEL_URL', 'shram-alerts.vercel.app')


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


def get_verified_subscribers():
    """Fetch all verified subscribers from Google Sheets."""
    if not GOOGLE_SHEETS_CREDENTIALS or not SHEET_ID:
        print("WARNING: Google Sheets credentials not configured. Skipping subscriber fetch.")
        return []

    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SHEET_ID).sheet1
        records = sheet.get_all_records()

        # Filter to verified subscribers only
        verified = [r for r in records if r.get('status') == 'verified']
        print(f"Found {len(verified)} verified subscribers")
        return verified

    except Exception as e:
        print(f"ERROR fetching subscribers: {e}")
        return []


# =============================================================================
# DATA LOADING FUNCTIONS
# =============================================================================

def load_grid_data():
    """Load current grid_data.json."""
    if not os.path.exists(GRID_DATA_PATH):
        print(f"ERROR: Grid data not found at {GRID_DATA_PATH}")
        return None

    with open(GRID_DATA_PATH, 'r') as f:
        return json.load(f)


def load_alert_state():
    """Load previous alert state to track which Zone 6 events have been alerted."""
    if os.path.exists(ALERT_STATE_PATH):
        try:
            with open(ALERT_STATE_PATH, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Could not load alert state: {e}")

    return {
        'last_check': None,
        'active_zone6_districts': {}
    }


def save_alert_state(state):
    """Save alert state to track Zone 6 events."""
    os.makedirs(os.path.dirname(ALERT_STATE_PATH), exist_ok=True)
    with open(ALERT_STATE_PATH, 'w') as f:
        json.dump(state, f, indent=2)


# =============================================================================
# ZONE 6 DETECTION
# =============================================================================

def get_zone6_districts(grid_data):
    """
    Extract all districts currently in Zone 6 for any MET level or sun/shade condition.

    Returns:
        dict: {district_name: {'state': state_name, 'met_levels': set, 'conditions': set}}
    """
    zone6_districts = {}

    for point in grid_data.get('points', []):
        district = point.get('district')
        state = point.get('state')

        if not district:
            continue

        # Check all MET levels and conditions
        for met in ['met3', 'met4', 'met5', 'met6']:
            for condition in ['shade', 'sun']:
                try:
                    zone = point['data'][met][condition]['zone']
                    if zone == 6:
                        if district not in zone6_districts:
                            zone6_districts[district] = {
                                'state': state,
                                'met_levels': set(),
                                'conditions': set()
                            }
                        zone6_districts[district]['met_levels'].add(met)
                        zone6_districts[district]['conditions'].add(condition)
                except (KeyError, TypeError):
                    continue

    return zone6_districts


def get_new_zone6_districts(current_zone6, previous_zone6):
    """
    Determine which districts are NEWLY in Zone 6.

    Args:
        current_zone6: Current Zone 6 districts from grid_data
        previous_zone6: Previously alerted Zone 6 districts from alert_state

    Returns:
        set: District names that are newly in Zone 6
    """
    current_districts = set(current_zone6.keys())
    previous_districts = set(previous_zone6.keys())

    # Districts that are in Zone 6 now but weren't before
    new_districts = current_districts - previous_districts

    return new_districts


# =============================================================================
# EMAIL FUNCTIONS
# =============================================================================

def send_zone6_alert(subscriber, districts_affected, zone6_info, metadata):
    """
    Send Zone 6 heat stress alert email to a subscriber.

    Args:
        subscriber: Subscriber record from Google Sheets
        districts_affected: List of district names in Zone 6
        zone6_info: Dict with zone 6 details for each district
        metadata: Grid data metadata (timestamp, etc.)
    """
    email = subscriber.get('email')
    name = subscriber.get('name', '')
    token = subscriber.get('verification_token')

    if not email:
        return False

    # Build district list HTML
    district_items = []
    for district in districts_affected:
        info = zone6_info.get(district, {})
        state = info.get('state', '')
        met_levels = info.get('met_levels', set())
        conditions = info.get('conditions', set())

        # Format MET levels
        met_str = ', '.join(sorted([m.replace('met', 'MET ') for m in met_levels]))
        cond_str = ' & '.join(sorted(conditions))

        district_items.append(f"""
            <li style="margin-bottom: 8px;">
                <strong>{district}</strong>{', ' + state if state else ''}
                <br><span style="font-size: 12px; color: #666;">({met_str} | {cond_str})</span>
            </li>
        """)

    district_list_html = '\n'.join(district_items)

    # Subject line
    if len(districts_affected) == 1:
        subject = f"🔴 SHRAM Alert: Zone 6 Heat Stress in {districts_affected[0]}"
    else:
        subject = f"🔴 SHRAM Alert: Zone 6 Heat Stress in {len(districts_affected)} Districts"

    # Build HTML email
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: 'Montserrat', Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .alert-header {{ background: linear-gradient(135deg, #7B1FA2 0%, #9C27B0 100%); color: white; padding: 25px; text-align: center; border-radius: 8px 8px 0 0; }}
            .content {{ background: #f8f9fa; padding: 25px; border-radius: 0 0 8px 8px; }}
            .district-list {{ background: white; padding: 15px; border-radius: 6px; border-left: 4px solid #7B1FA2; }}
            .action-box {{ background: #fff3e0; padding: 15px; border-radius: 6px; margin: 20px 0; border-left: 4px solid #e65100; }}
            .btn {{ display: inline-block; background: #006D77; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; font-weight: 600; }}
            .footer {{ font-size: 12px; color: #666; margin-top: 25px; padding-top: 20px; border-top: 1px solid #ddd; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="alert-header">
                <h1 style="margin: 0; font-size: 24px;">⚠️ Zone 6 Heat Alert</h1>
                <p style="margin: 10px 0 0 0; opacity: 0.9;">Hazardous Heat Stress Detected</p>
            </div>
            <div class="content">
                <p>Hello{', ' + name if name else ''},</p>

                <p><strong>Hazardous heat conditions (Zone 6)</strong> have been detected in your subscribed district(s):</p>

                <div class="district-list">
                    <ul style="margin: 0; padding-left: 20px;">
                        {district_list_html}
                    </ul>
                </div>

                <div class="action-box">
                    <h3 style="margin: 0 0 10px 0; color: #e65100;">🛡️ Protective Actions</h3>
                    <ul style="margin: 0; padding-left: 20px;">
                        <li><strong>Stop outdoor work immediately</strong> if possible</li>
                        <li>Move to a cooler, shaded location</li>
                        <li>Drink water frequently</li>
                        <li>Watch for signs of heat illness (dizziness, nausea, rapid heartbeat)</li>
                        <li>Seek medical help if symptoms occur</li>
                    </ul>
                </div>

                <p style="text-align: center; margin: 25px 0;">
                    <a href="{DASHBOARD_URL}" class="btn">View Live Dashboard</a>
                </p>

                <p style="font-size: 13px; color: #666;">
                    Data updated: {metadata.get('generated_at_ist', 'Unknown')}
                </p>

                <div class="footer">
                    <p>
                        You're receiving this because you subscribed to SHRAM heat alerts.
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
    text_body = f"""
    ZONE 6 HEAT ALERT - Hazardous Conditions

    Hello{', ' + name if name else ''},

    Hazardous heat conditions (Zone 6) have been detected in your subscribed district(s):
    {', '.join(districts_affected)}

    PROTECTIVE ACTIONS:
    - Stop outdoor work immediately if possible
    - Move to a cooler, shaded location
    - Drink water frequently
    - Watch for signs of heat illness
    - Seek medical help if symptoms occur

    View live dashboard: {DASHBOARD_URL}

    Data updated: {metadata.get('generated_at_ist', 'Unknown')}

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

        print(f"  ✓ Alert sent to {email}")
        return True

    except Exception as e:
        print(f"  ✗ Failed to send to {email}: {e}")
        return False


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    """Main alert sending logic."""
    print("=" * 60)
    print("SHRAM Zone 6 Alert System")
    print("=" * 60)

    # Check credentials
    if not EMAIL_PASSWORD:
        print("ERROR: GMAIL_APP_PASSWORD not set. Cannot send emails.")
        return

    # Load grid data
    print("\n[1/5] Loading grid data...")
    grid_data = load_grid_data()
    if not grid_data:
        print("ERROR: Could not load grid data. Exiting.")
        return

    print(f"  Loaded {len(grid_data.get('points', []))} points")
    print(f"  Generated at: {grid_data.get('metadata', {}).get('generated_at_ist', 'Unknown')}")

    # Find Zone 6 districts
    print("\n[2/5] Checking for Zone 6 conditions...")
    current_zone6 = get_zone6_districts(grid_data)

    if not current_zone6:
        print("  No Zone 6 conditions detected. No alerts needed.")
        # Still update state to clear any previous Zone 6 districts
        state = load_alert_state()
        state['last_check'] = datetime.now(pytz.timezone('Asia/Kolkata')).isoformat()
        state['active_zone6_districts'] = {}
        save_alert_state(state)
        print("\n✓ Alert check complete. No alerts sent.")
        return

    print(f"  Zone 6 detected in {len(current_zone6)} districts:")
    for district in sorted(current_zone6.keys())[:10]:
        print(f"    - {district}")
    if len(current_zone6) > 10:
        print(f"    ... and {len(current_zone6) - 10} more")

    # Check for NEW Zone 6 events
    print("\n[3/5] Checking for NEW Zone 6 events...")
    state = load_alert_state()
    previous_zone6 = state.get('active_zone6_districts', {})

    new_zone6 = get_new_zone6_districts(current_zone6, previous_zone6)

    if not new_zone6:
        print("  No NEW Zone 6 events. All current Zone 6 districts were already alerted.")
        # Update state with current districts
        state['last_check'] = datetime.now(pytz.timezone('Asia/Kolkata')).isoformat()
        state['active_zone6_districts'] = {
            d: {'state': info.get('state')}
            for d, info in current_zone6.items()
        }
        save_alert_state(state)
        print("\n✓ Alert check complete. No new alerts sent.")
        return

    print(f"  NEW Zone 6 events in {len(new_zone6)} districts:")
    for district in sorted(new_zone6):
        print(f"    - {district}")

    # Get verified subscribers
    print("\n[4/5] Fetching verified subscribers...")
    subscribers = get_verified_subscribers()

    if not subscribers:
        print("  No verified subscribers found.")
        # Still update state
        state['last_check'] = datetime.now(pytz.timezone('Asia/Kolkata')).isoformat()
        state['active_zone6_districts'] = {
            d: {'state': info.get('state')}
            for d, info in current_zone6.items()
        }
        save_alert_state(state)
        print("\n✓ Alert check complete. No subscribers to notify.")
        return

    # Send alerts to matching subscribers
    print(f"\n[5/5] Sending alerts to subscribers...")
    alerts_sent = 0
    alerts_failed = 0

    for subscriber in subscribers:
        # Parse subscriber's districts
        sub_districts_str = subscriber.get('districts', '')
        sub_districts = set(d.strip() for d in sub_districts_str.split(',') if d.strip())

        # Find matching districts that are newly in Zone 6
        matching = sub_districts & new_zone6

        if matching:
            print(f"\n  Alerting {subscriber.get('email')} for: {', '.join(matching)}")
            success = send_zone6_alert(
                subscriber,
                list(matching),
                {d: current_zone6[d] for d in matching},
                grid_data.get('metadata', {})
            )
            if success:
                alerts_sent += 1
            else:
                alerts_failed += 1

    # Update alert state
    state['last_check'] = datetime.now(pytz.timezone('Asia/Kolkata')).isoformat()
    state['active_zone6_districts'] = {
        d: {'state': info.get('state'), 'first_detected': datetime.now(pytz.timezone('Asia/Kolkata')).isoformat()}
        for d, info in current_zone6.items()
    }
    save_alert_state(state)

    # Summary
    print("\n" + "=" * 60)
    print("ALERT SUMMARY")
    print("=" * 60)
    print(f"  Zone 6 districts: {len(current_zone6)}")
    print(f"  NEW Zone 6 events: {len(new_zone6)}")
    print(f"  Verified subscribers: {len(subscribers)}")
    print(f"  Alerts sent: {alerts_sent}")
    if alerts_failed:
        print(f"  Alerts failed: {alerts_failed}")
    print("=" * 60)


if __name__ == '__main__':
    main()
