#!/usr/bin/env python3
"""
Send Heat Stress Alerts to Verified Subscribers

This script runs after generate_grid_data_openmeteo.py in GitHub Actions.
It checks for NEW zone conditions matching subscriber preferences and sends instant alerts.

Key Features:
- Respects subscriber MET level preferences (only alerts for their chosen MET levels)
- Respects subscriber zone preferences (Zone 4, 5, 6 - Zone 6 always included)
- Respects subscriber sun/shade preferences
- CRITICAL: During nighttime (6 PM - 6 AM IST), always uses shade data regardless of preference
- Deduplication: Only alerts when a zone is NEWLY detected (not for persistent conditions)

Usage:
    python send_alerts.py

Environment Variables Required:
    GMAIL_ADDRESS - Gmail address to send from (e.g., shram.alerts@gmail.com)
    GMAIL_APP_PASSWORD - Gmail App Password for authentication
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

# Email settings (using Gmail SMTP)
GMAIL_ADDRESS = os.environ.get('GMAIL_ADDRESS', 'shram.alerts@gmail.com')
GMAIL_APP_PASSWORD = os.environ.get('GMAIL_APP_PASSWORD')

# Google Sheets
GOOGLE_SHEETS_CREDENTIALS = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
SHEET_ID = os.environ.get('SHEET_ID')

# File paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)  # Parent of scripts/
GRID_DATA_PATH = os.path.join(ROOT_DIR, 'grid_data.json')
ALERT_STATE_PATH = os.path.join(ROOT_DIR, 'weather_logs', 'alert_state.json')
ALERT_HISTORY_PATH = os.path.join(ROOT_DIR, 'weather_logs', 'alert_history.json')

# Dashboard URL
DASHBOARD_URL = 'https://shram.info'

# Vercel URL for unsubscribe links
VERCEL_URL = os.environ.get('VERCEL_URL', 'shram-alerts.vercel.app')

# IST timezone
IST = pytz.timezone('Asia/Kolkata')

# Zone colors for email styling (matching dashboard colors, accessible text)
ZONE_COLORS = {
    4: {'bg': '#fff3e0', 'border': '#e65100', 'text': '#C24400', 'name': 'Moderate', 'subject': 'Moderate Risk'},
    5: {'bg': '#ffebee', 'border': '#d32f2f', 'text': '#CA2B2B', 'name': 'High', 'subject': 'High Risk'},
    6: {'bg': '#f3e5f5', 'border': '#7B1FA2', 'text': '#7B1FA2', 'name': 'Hazardous', 'subject': 'Hazardous'}
}


# =============================================================================
# NIGHTTIME DETECTION
# =============================================================================

def is_nighttime_ist():
    """
    Check if current IST time is during nighttime (6 PM to 6 AM).
    During nighttime, sun values equal shade values (no solar radiation).

    Returns:
        bool: True if nighttime (6 PM to 6 AM IST), False otherwise
    """
    now_ist = datetime.now(IST)
    hour = now_ist.hour
    # Nighttime is 18:00 (6 PM) to 05:59 (before 6 AM)
    return hour >= 18 or hour < 6


def get_effective_sun_shade(subscriber_preference, is_night):
    """
    Determine which sun/shade condition to use for alerting.

    Args:
        subscriber_preference: 'sun', 'shade', or 'both'
        is_night: Whether it's currently nighttime in IST

    Returns:
        list: List of conditions to check ['shade'], ['sun'], or ['sun', 'shade']
    """
    if is_night:
        # At night, always use shade (no solar radiation)
        return ['shade']

    if subscriber_preference == 'both':
        return ['sun', 'shade']
    elif subscriber_preference == 'sun':
        return ['sun']
    else:  # 'shade' or default
        return ['shade']


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
    """
    Fetch all verified subscribers from Google Sheets.

    Returns:
        list: List of subscriber records with parsed preferences (includes row_num for updates)
    """
    if not GOOGLE_SHEETS_CREDENTIALS or not SHEET_ID:
        print("WARNING: Google Sheets credentials not configured. Skipping subscriber fetch.")
        return []

    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SHEET_ID).sheet1
        records = sheet.get_all_records()

        # Filter to verified subscribers and parse preferences
        verified = []
        for idx, r in enumerate(records):
            if r.get('status') != 'verified':
                continue

            # Parse MET levels (stored as "'3,4,5,6" or "3456" if Google Sheets stripped commas)
            met_levels_str = str(r.get('met_levels', '6')).lstrip("'")  # Strip leading apostrophe if present
            if ',' in met_levels_str:
                met_levels = [int(m.strip()) for m in met_levels_str.split(',') if m.strip().isdigit()]
            else:
                # Handle case where Google Sheets stored as number without commas (e.g., "3456" -> [3,4,5,6])
                met_levels = [int(d) for d in met_levels_str if d.isdigit() and 3 <= int(d) <= 6]
            if not met_levels:
                met_levels = [6]  # Default to MET 6

            # Parse alert zones (stored as "'4,5,6" or "456" if Google Sheets stripped commas)
            alert_zones_str = str(r.get('alert_zones', '6')).lstrip("'")  # Strip leading apostrophe if present
            if ',' in alert_zones_str:
                alert_zones = [int(z.strip()) for z in alert_zones_str.split(',') if z.strip().isdigit()]
            else:
                # Handle case where Google Sheets stored as number without commas (e.g., "456" -> [4,5,6])
                alert_zones = [int(d) for d in alert_zones_str if d.isdigit() and 4 <= int(d) <= 6]
            if not alert_zones:
                alert_zones = [6]  # Default to Zone 6

            # Parse sun/shade preference
            sun_shade = r.get('sun_shade', 'shade')
            if sun_shade not in ['sun', 'shade', 'both']:
                sun_shade = 'shade'

            # Parse districts
            districts_str = r.get('districts', '')
            districts = [d.strip() for d in districts_str.split(',') if d.strip()]

            verified.append({
                'email': r.get('email'),
                'name': r.get('name', ''),
                'phone': r.get('phone', ''),  # For future SMS alerts
                'districts': districts,
                'met_levels': met_levels,
                'alert_zones': alert_zones,
                'sun_shade': sun_shade,
                'verification_token': r.get('verification_token'),
                'receive_forecasts': r.get('receive_forecasts', 'yes') == 'yes',
                'receive_sms': r.get('receive_sms', 'no') == 'yes',  # For future SMS alerts
                'row_num': idx + 2  # Row number in sheet (1-indexed, +1 for header)
            })

        print(f"Found {len(verified)} verified subscribers")
        return verified

    except Exception as e:
        print(f"ERROR fetching subscribers: {e}")
        return []


def update_last_alert_sent(row_num, timestamp):
    """
    Update the last_alert_sent column (column 14) for a subscriber.

    Args:
        row_num: Row number in the sheet (1-indexed)
        timestamp: ISO format timestamp to set
    """
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SHEET_ID).sheet1
        # Column 14 is last_alert_sent
        sheet.update_cell(row_num, 14, timestamp)
        print(f"    Updated last_alert_sent for row {row_num}")
    except Exception as e:
        print(f"    Warning: Could not update last_alert_sent: {e}")


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
    """Load previous alert state to track which zone events have been alerted."""
    if os.path.exists(ALERT_STATE_PATH):
        try:
            with open(ALERT_STATE_PATH, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Could not load alert state: {e}")

    return {
        'last_check': None,
        'active_alerts': {}  # {subscriber_email: {district: {met: zone_level}}}
    }


def save_alert_state(state):
    """Save alert state to track zone events."""
    os.makedirs(os.path.dirname(ALERT_STATE_PATH), exist_ok=True)
    with open(ALERT_STATE_PATH, 'w') as f:
        json.dump(state, f, indent=2)


def load_alert_history():
    """Load alert history for appending new entries."""
    if os.path.exists(ALERT_HISTORY_PATH):
        try:
            with open(ALERT_HISTORY_PATH, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Could not load alert history: {e}")
    return {'alerts': []}


def log_alert(subscriber_email, subscriber_name, alerts, is_night, success):
    """
    Log an alert to the alert history file.

    Args:
        subscriber_email: Email of the subscriber
        subscriber_name: Name of the subscriber (may be empty)
        alerts: List of alert items sent
        is_night: Whether it was nighttime
        success: Whether the email was sent successfully
    """
    history = load_alert_history()

    now_ist = datetime.now(IST)

    # Create log entry
    entry = {
        'timestamp': now_ist.isoformat(),
        'subscriber_email': subscriber_email,
        'subscriber_name': subscriber_name,
        'districts': list(set(a['district'] for a in alerts)),
        'zones': list(set(a['zone'] for a in alerts)),
        'met_levels': list(set(a['met_level'] for a in alerts)),
        'conditions': list(set(a['condition'] for a in alerts)),
        'is_nighttime': is_night,
        'success': success,
        'alert_count': len(alerts)
    }

    history['alerts'].append(entry)

    # Save updated history (kept indefinitely for analysis)
    os.makedirs(os.path.dirname(ALERT_HISTORY_PATH), exist_ok=True)
    with open(ALERT_HISTORY_PATH, 'w') as f:
        json.dump(history, f, indent=2)


# =============================================================================
# ZONE DETECTION
# =============================================================================

def get_district_zones(grid_data, is_night):
    """
    Extract zone levels for all districts from grid_data.

    Args:
        grid_data: Loaded grid_data.json
        is_night: Whether it's nighttime (affects sun/shade logic)

    Returns:
        dict: {district_name: {state, temp, rh, zones: {met3: {shade: zone, sun: zone}, ...}}}
    """
    district_data = {}

    for point in grid_data.get('points', []):
        district = point.get('district')
        if not district:
            continue

        # Get zone data for each MET level
        zones = {}
        for met_num in [3, 4, 5, 6]:
            met_key = f'met{met_num}'
            zones[met_num] = {}

            for condition in ['shade', 'sun']:
                try:
                    zone = point['data'][met_key][condition]['zone']
                    zones[met_num][condition] = zone
                except (KeyError, TypeError):
                    zones[met_num][condition] = None

        # Store or update district data (use highest zones if multiple points)
        if district not in district_data:
            district_data[district] = {
                'state': point.get('state'),
                'temp': point.get('temp'),
                'rh': point.get('rh'),
                'zones': zones
            }
        else:
            # Update to higher zone if this point has worse conditions
            for met_num in [3, 4, 5, 6]:
                for condition in ['shade', 'sun']:
                    existing = district_data[district]['zones'][met_num].get(condition)
                    new_zone = zones[met_num].get(condition)
                    if new_zone is not None:
                        if existing is None or new_zone > existing:
                            district_data[district]['zones'][met_num][condition] = new_zone

    return district_data


def check_subscriber_alerts(subscriber, district_data, previous_alerts, is_night):
    """
    Check which alerts should be sent to a subscriber based on their preferences.

    Args:
        subscriber: Subscriber record with preferences
        district_data: Zone data for all districts
        previous_alerts: Previously sent alerts for this subscriber
        is_night: Whether it's nighttime

    Returns:
        list: List of alert items to send, each with district, met, zone, condition info
    """
    alerts_to_send = []

    # Get effective sun/shade conditions to check
    conditions_to_check = get_effective_sun_shade(subscriber['sun_shade'], is_night)

    for district in subscriber['districts']:
        if district not in district_data:
            continue

        data = district_data[district]

        # Check each MET level the subscriber cares about
        for met_level in subscriber['met_levels']:
            if met_level not in data['zones']:
                continue

            # Check each condition (sun/shade based on preference and time of day)
            for condition in conditions_to_check:
                zone = data['zones'][met_level].get(condition)
                if zone is None:
                    continue

                # Check if this zone level is in subscriber's alert zones
                if zone not in subscriber['alert_zones']:
                    continue

                # Check if this is a NEW alert (deduplication)
                prev_key = f"{district}_{met_level}_{condition}"
                prev_zone = previous_alerts.get(prev_key)

                if prev_zone != zone:
                    # This is a new or changed zone - send alert
                    alerts_to_send.append({
                        'district': district,
                        'state': data['state'],
                        'met_level': met_level,
                        'zone': zone,
                        'condition': condition,
                        'temp': data['temp'],
                        'rh': data['rh']
                    })

    return alerts_to_send


# =============================================================================
# EMAIL FUNCTIONS
# =============================================================================

def send_alert_email(subscriber, alerts, metadata, is_night):
    """
    Send heat stress alert email to a subscriber.

    Args:
        subscriber: Subscriber record
        alerts: List of alert items to include
        metadata: Grid data metadata
        is_night: Whether it's nighttime
    """
    email = subscriber.get('email')
    name = subscriber.get('name', '')
    token = subscriber.get('verification_token')

    if not email or not alerts:
        return False

    # Group alerts by zone level for display
    alerts_by_zone = {4: [], 5: [], 6: []}
    for alert in alerts:
        zone = alert['zone']
        if zone in alerts_by_zone:
            alerts_by_zone[zone].append(alert)

    # Find highest zone for subject line
    highest_zone = max(a['zone'] for a in alerts)
    zone_info = ZONE_COLORS[highest_zone]

    # Build district list HTML
    alert_sections = []
    for zone in [6, 5, 4]:  # Show highest zones first
        if not alerts_by_zone[zone]:
            continue

        color = ZONE_COLORS[zone]
        district_items = []
        for alert in alerts_by_zone[zone]:
            met_label = f"MET {alert['met_level']}"
            cond_icon = "Shade" if alert['condition'] == 'shade' else "Sun"
            if is_night and alert['condition'] == 'shade':
                cond_icon = "Night"

            district_items.append(f"""
                <li style="margin-bottom: 8px;">
                    <strong>{alert['district']}</strong>{', ' + alert['state'] if alert['state'] else ''}
                    <br><span style="font-size: 12px; color: #666;">
                        {met_label} | {cond_icon} | {alert['temp']:.1f}°C, {alert['rh']}% RH
                    </span>
                </li>
            """)

        alert_sections.append(f"""
            <div style="margin-bottom: 20px; padding: 15px; background: {color['bg']}; border-left: 4px solid {color['border']}; border-radius: 6px;">
                <h3 style="margin: 0 0 10px 0; color: {color['text']};">Zone {zone} - {color['name']}</h3>
                <ul style="margin: 0; padding-left: 20px;">
                    {''.join(district_items)}
                </ul>
            </div>
        """)

    # Subject line with timestamp to prevent Gmail threading
    now_ist = datetime.now(IST)
    time_str = now_ist.strftime('%b %d, %I:%M %p IST')
    district_names = list(set(a['district'] for a in alerts))
    if len(district_names) == 1:
        subject = f"SHRAM Alert: Zone {highest_zone} {zone_info['subject']} - {district_names[0]} ({time_str})"
    else:
        subject = f"SHRAM Alert: Zone {highest_zone} {zone_info['subject']} - {len(district_names)} Districts ({time_str})"

    # Nighttime notice
    nighttime_notice = ""
    if is_night:
        nighttime_notice = """
            <p style="background: #e3f2fd; padding: 10px; border-radius: 6px; font-size: 13px;">
                <strong>Nighttime Alert:</strong> This alert uses shade values as there is no solar radiation between 6 PM and 6 AM IST.
            </p>
        """

    # Build HTML email
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: 'Montserrat', Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .alert-header {{ background: linear-gradient(135deg, {zone_info['border']} 0%, {zone_info['text']} 100%); color: white; padding: 25px; text-align: center; border-radius: 8px 8px 0 0; }}
            .content {{ background: #f8f9fa; padding: 25px; border-radius: 0 0 8px 8px; }}
            .action-box {{ background: #e0f2f1; padding: 15px; border-radius: 6px; margin: 20px 0; border-left: 4px solid #006D77; }}
            .btn {{ display: inline-block; background: #006D77; color: #ffffff !important; padding: 12px 24px; text-decoration: none; border-radius: 6px; font-weight: 600; }}
            .footer {{ font-size: 12px; color: #666; margin-top: 25px; padding-top: 20px; border-top: 1px solid #ddd; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="alert-header">
                <h1 style="margin: 0; font-size: 24px;">Zone {highest_zone} {zone_info['subject']} Heat Alert</h1>
                <p style="margin: 10px 0 0 0; opacity: 0.9;">{zone_info['name']} Heat Stress Detected</p>
            </div>
            <div class="content">
                <p>Hello{' ' + name if name else ''},</p>

                <p><strong>{zone_info['name']} heat stress conditions (Zone {highest_zone})</strong> have been detected in your subscribed district(s):</p>

                {nighttime_notice}

                {''.join(alert_sections)}

                <div class="action-box">
                    <h3 style="margin: 0 0 10px 0; color: #006D77;">Recommended Actions</h3>
                    <ul style="margin: 0; padding-left: 20px;">
                        {'<li><strong>Stop outdoor work immediately</strong> if possible</li>' if highest_zone == 6 else '<li><strong>Take extra precautions</strong> during work</li>'}
                        <li>Move to a cooler, shaded location</li>
                        <li>Drink water frequently</li>
                        <li>Watch for signs of heat illness (e.g., dizziness, nausea, rapid heartbeat)</li>
                        {'<li>Seek medical help if symptoms occur</li>' if highest_zone >= 5 else ''}
                    </ul>
                </div>

                <p style="text-align: center; margin: 25px 0;">
                    <a href="{DASHBOARD_URL}" class="btn" style="color: #ffffff !important;">View Live Dashboard</a>
                </p>

                <p style="font-size: 13px; color: #666;">
                    Data updated: {metadata.get('generated_at_ist', 'Unknown')}
                </p>

                <div class="footer">
                    <p>
                        You're receiving this because you subscribed to SHRAM heat alerts for Zone {', '.join(str(z) for z in sorted(subscriber['alert_zones']))} at MET {', '.join(str(m) for m in sorted(subscriber['met_levels']))}.
                        <br>
                        <a href="{DASHBOARD_URL}/preferences.html?token={token}">Update Preferences</a> |
                        <a href="https://shram-alerts.vercel.app/api/unsubscribe?token={token}">Unsubscribe</a> |
                        <a href="{DASHBOARD_URL}">SHRAM Dashboard</a>
                    </p>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

    # Plain text version
    district_list = ', '.join(district_names)
    text_body = f"""
ZONE {highest_zone} {zone_info['subject'].upper()} HEAT ALERT

Hello{' ' + name if name else ''},

{zone_info['name']} heat stress conditions (Zone {highest_zone}) have been detected in your subscribed district(s):
{district_list}

{'Note: This is a nighttime alert. Shade values are used as there is no solar radiation between 6 PM and 6 AM IST.' if is_night else ''}

RECOMMENDED ACTIONS:
- {'Stop outdoor work immediately if possible' if highest_zone == 6 else 'Take extra precautions during work'}
- Move to a cooler, shaded location
- Drink water frequently
- Watch for signs of heat illness (e.g., dizziness, nausea, rapid heartbeat)
{'- Seek medical help if symptoms occur' if highest_zone >= 5 else ''}

View live dashboard: {DASHBOARD_URL}

Data updated: {metadata.get('generated_at_ist', 'Unknown')}

---
Unsubscribe: https://shram-alerts.vercel.app/api/unsubscribe?token={token}
    """

    # Send email via Gmail SMTP
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = f"SHRAM Alerts <{GMAIL_ADDRESS}>"
        msg['To'] = email

        # Attach both plain text and HTML versions
        part1 = MIMEText(text_body, 'plain')
        part2 = MIMEText(html_body, 'html')
        msg.attach(part1)
        msg.attach(part2)

        # Connect to Gmail SMTP and send
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_ADDRESS, email, msg.as_string())

        print(f"  ✓ Alert sent to {email} ({len(alerts)} districts)")
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
    print("SHRAM Heat Alert System")
    print("=" * 60)

    # Check if nighttime
    is_night = is_nighttime_ist()
    now_ist = datetime.now(IST)
    print(f"\nCurrent IST time: {now_ist.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Nighttime mode: {'Yes (using shade values)' if is_night else 'No (respecting sun/shade preferences)'}")

    # Check credentials
    if not GMAIL_APP_PASSWORD:
        print("\nERROR: GMAIL_APP_PASSWORD not set. Cannot send emails.")
        return

    # Load grid data
    print("\n[1/5] Loading grid data...")
    grid_data = load_grid_data()
    if not grid_data:
        print("ERROR: Could not load grid data. Exiting.")
        return

    print(f"  Loaded {len(grid_data.get('points', []))} points")
    print(f"  Generated at: {grid_data.get('metadata', {}).get('generated_at_ist', 'Unknown')}")

    # Extract district zone data
    print("\n[2/5] Analyzing zone conditions...")
    district_data = get_district_zones(grid_data, is_night)
    print(f"  Found data for {len(district_data)} districts")

    # Count districts in each zone
    zone_counts = {4: 0, 5: 0, 6: 0}
    for district, data in district_data.items():
        for met in data['zones'].values():
            for zone in met.values():
                if zone in zone_counts:
                    zone_counts[zone] = max(zone_counts[zone], 1)  # Just count as present

    # Get verified subscribers
    print("\n[3/5] Fetching verified subscribers...")
    subscribers = get_verified_subscribers()

    if not subscribers:
        print("  No verified subscribers found. Exiting.")
        return

    # Load previous alert state
    print("\n[4/5] Checking for new alerts...")
    state = load_alert_state()

    # Track alerts sent
    alerts_sent = 0
    alerts_failed = 0
    new_state = {
        'last_check': now_ist.isoformat(),
        'active_alerts': {}
    }

    print("\n[5/5] Processing subscribers...")
    for subscriber in subscribers:
        email = subscriber['email']

        # Get previous alerts for this subscriber
        prev_alerts = state.get('active_alerts', {}).get(email, {})

        # Check what alerts should be sent
        alerts = check_subscriber_alerts(subscriber, district_data, prev_alerts, is_night)

        # Update state with current conditions
        new_state['active_alerts'][email] = {}
        for district in subscriber['districts']:
            if district not in district_data:
                continue
            data = district_data[district]
            for met_level in subscriber['met_levels']:
                if met_level not in data['zones']:
                    continue
                conditions = get_effective_sun_shade(subscriber['sun_shade'], is_night)
                for condition in conditions:
                    zone = data['zones'][met_level].get(condition)
                    if zone and zone in subscriber['alert_zones']:
                        key = f"{district}_{met_level}_{condition}"
                        new_state['active_alerts'][email][key] = zone

        if alerts:
            print(f"\n  Alerting {email}:")
            for a in alerts:
                print(f"    - {a['district']}: Zone {a['zone']} (MET {a['met_level']}, {a['condition']})")

            success = send_alert_email(
                subscriber,
                alerts,
                grid_data.get('metadata', {}),
                is_night
            )

            # Log the alert to history
            log_alert(
                subscriber_email=email,
                subscriber_name=subscriber.get('name', ''),
                alerts=alerts,
                is_night=is_night,
                success=success
            )

            if success:
                alerts_sent += 1
                # Update last_alert_sent in Google Sheets
                update_last_alert_sent(subscriber['row_num'], now_ist.isoformat())
            else:
                alerts_failed += 1

    # Save new state
    save_alert_state(new_state)

    # Summary
    print("\n" + "=" * 60)
    print("ALERT SUMMARY")
    print("=" * 60)
    print(f"  Districts analyzed: {len(district_data)}")
    print(f"  Verified subscribers: {len(subscribers)}")
    print(f"  Alerts sent: {alerts_sent}")
    if alerts_failed:
        print(f"  Alerts failed: {alerts_failed}")
    print(f"  Nighttime mode: {'Yes' if is_night else 'No'}")
    print("=" * 60)


if __name__ == '__main__':
    main()
