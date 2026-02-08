import os
import requests
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import math
import numpy as np
import json

# Path setup
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)  # Parent of scripts/

# JPL version is pure Python, no NumbaMinpack needed
from heatindex_jpl import extendedheatindex as heatindex_jpl
from heatindex_jpl import find_eqvar as ehi_zone
from heatindex_jpl import pvstar as pvstar_jpl
from heatindex_jpl import Pa0

# Use lookup tables for EK calculations (no NumbaMinpack/scipy needed)
from ehi_lookup import EHILookup
ehi_lookup = EHILookup()
print("Using EHI lookup tables for EK calculations")

# MET levels in W/m² (converting from MET units where 1 MET ≈ 58 W/m²)
MET_LEVELS = {
    3: 180,   # Light work (~3 MET)
    4: 240,   # Moderate work (~4 MET)
    5: 300,   # Heavy work (~5 MET)
    6: 360    # Very heavy work (~6 MET)
}

# from pilotehi350 import modifiedheatindex as heatindex_ek
# from pilotehi350 import find_eqvar as ehi350_zone
# from pilotehi350 import pvstar as pvstar_ek

# Email imports disabled - using send_alerts.py instead
# import smtplib
# from email.message import EmailMessage
from tabulate import tabulate 
from bs4 import BeautifulSoup


def format_name(name):
    """Convert IMD format names to proper title case.

    Examples:
        ANDHRA_PRADESH -> Andhra Pradesh
        NEW_DELHI -> New Delhi
        MUMBAI -> Mumbai
    """
    if pd.isna(name) or not isinstance(name, str):
        return name
    # Replace underscores with spaces and convert to title case
    return name.replace('_', ' ').title()


# Email sending disabled - using send_alerts.py instead (via update-grid-data.yml workflow)
# EMAIL_SENDER = "eliana101299@gmail.com"
# EMAIL_PASSWORD = "xpyv cwzn drfk ewef"
# EMAIL_RECIPIENTS = ["elifkilic@berkeley.edu", "eliana101299@gmail.com", "rohini.tamarana@berkeley.edu"]
# SMTP_SERVER = "smtp.gmail.com"
# SMTP_PORT = 587

def get_weekly_filename():
    """Generate filename based on ISO week number"""
    now = datetime.now(ZoneInfo("Asia/Calcutta"))
    year = now.year
    week = now.isocalendar()[1]  # ISO week number
    return os.path.join(ROOT_DIR, 'weather_logs', f"india_weather_{year}_week{week:02d}.csv")

def fetch_and_log():
    try:

        temp_url = "http://aws.imd.gov.in:8091/AWS/hometemp.php?a=60&b=ALL_STATE"
        temp_data = [r.split(",") for r in requests.get(temp_url).json()]
        temp_df = pd.DataFrame(temp_data, columns=["LAT", "LON", "TYPE", "STATE", "DISTRICT", "STATION", "TEMP", "DATE_TEMP", "TIME_TEMP", "EXTRA"])
        temp_df["TEMP"] = pd.to_numeric(temp_df["TEMP"], errors="coerce")

        # Fetch RH data
        rh_url = "http://aws.imd.gov.in:8091/AWS/homerh.php?a=60&b=ALL_STATE"
        rh_data = [r.split(",") for r in requests.get(rh_url).json()]
        rh_df = pd.DataFrame(rh_data, columns=["LAT", "LON", "TYPE", "STATE", "DISTRICT", "STATION", "RH", "DATE_RH", "TIME_RH", "EXTRA"])
        rh_df["RH"] = pd.to_numeric(rh_df["RH"], errors="coerce")

        # Merge only on STATION (keep timestamps separate)
        merged_df = pd.merge(
            temp_df[["STATE", "DISTRICT", "STATION", "TEMP", "DATE_TEMP", "TIME_TEMP"]],
            rh_df[["STATE", "DISTRICT", "STATION", "RH", "DATE_RH", "TIME_RH"]],
            on=["STATE", "DISTRICT", "STATION"],
            how="outer"
        )
        # Add timestamp of when data was logged
        # merged_df["LOGGED_AT (UTC)"] = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S")
        merged_df["LOGGED_AT (IST)"] = datetime.now(ZoneInfo("Asia/Calcutta")).strftime("%Y-%m-%d %H:%M:%S")

        # Format names to title case (e.g., ANDHRA_PRADESH -> Andhra Pradesh)
        merged_df["STATE"] = merged_df["STATE"].apply(format_name)
        merged_df["DISTRICT"] = merged_df["DISTRICT"].apply(format_name)
        merged_df["STATION"] = merged_df["STATION"].apply(format_name)

        def old_ehi(row):
            try:
                temp_k = row["TEMP"] + 273.15 if pd.notna(row["TEMP"]) else None
                rh_decimal = row["RH"] / 100 if pd.notna(row["RH"]) else None
                if temp_k is not None and rh_decimal is not None:
                    ehi = heatindex_jpl(temp_k, rh_decimal)
                    eqvar_name = ehi_zone(temp_k, rh_decimal)[0]
                    dic = {"phi":"Zone 1","Rf":"Zone 2 or 3","Rs":"Zone 4","Rs*":"Zone 5","dTcdt":"Zone 6"}
                    if eqvar_name == "Rf" and Pa0 > pvstar_jpl(ehi):
                        zone = "Zone 2"
                    elif eqvar_name == "Rf" and Pa0 <= pvstar_jpl(ehi):
                        zone = "Zone 3"
                    else:
                        zone = dic[eqvar_name]
                    ehi = round(ehi, 1)-273.15
                    return pd.Series([ehi, zone])
                else:
                    return pd.Series([None, None])
            except:
                return pd.Series([None, None])

        def new_ehi(row):
            """Calculate EHI for MET 350 W/m² (use MET 6 = 360 W/m² from lookup tables)."""
            try:
                temp_c = row["TEMP"] if pd.notna(row["TEMP"]) else None
                rh_percent = row["RH"] if pd.notna(row["RH"]) else None
                if temp_c is not None and rh_percent is not None:
                    # MET 350 is closest to MET 6 (360 W/m²), use shade
                    ehi, zone_num = ehi_lookup.get_ehi_zone(temp_c, rh_percent, 6, 'shade')
                    zone_map = {1: "Zone 1", 2: "Zone 2", 3: "Zone 3", 4: "Zone 4", 5: "Zone 5", 6: "Zone 6"}
                    zone = zone_map.get(zone_num, "Unknown")
                    return pd.Series([ehi, zone])
                else:
                    return pd.Series([None, None])
            except:
                return pd.Series([None, None])

        def calc_ehi_zone(row, met_level, sun_condition):
            """Calculate EHI and zone for any MET level and sun condition using lookup tables.

            Args:
                row: DataFrame row with TEMP and RH
                met_level: 3, 4, 5, or 6 (MET level)
                sun_condition: 'shade' or 'sun'

            Returns:
                pd.Series([ehi_value, zone_string])
            """
            try:
                temp_c = row["TEMP"] if pd.notna(row["TEMP"]) else None
                rh_percent = row["RH"] if pd.notna(row["RH"]) else None
                if temp_c is not None and rh_percent is not None:
                    ehi, zone_num = ehi_lookup.get_ehi_zone(temp_c, rh_percent, met_level, sun_condition)
                    zone_map = {1: "Zone 1", 2: "Zone 2", 3: "Zone 3", 4: "Zone 4", 5: "Zone 5", 6: "Zone 6"}
                    zone = zone_map.get(zone_num, "Unknown")
                    return pd.Series([ehi, zone])
                else:
                    return pd.Series([None, None])
            except:
                return pd.Series([None, None])

        merged_df[["EHI (in shade °C)", "Light Work Heat Stress Zone"]] = merged_df.apply(old_ehi, axis=1)
        merged_df[["EHI_350 (in shade °C)", "Hard Labor Heat Stress Zone"]] = merged_df.apply(new_ehi, axis=1)

        # Calculate EHI zones for all MET levels (3-6) and sun conditions (shade/sun)
        print("Calculating EHI for all MET levels and sun conditions...")
        for met in [3, 4, 5, 6]:
            for sun in ['shade', 'sun']:
                col_ehi = f"EHI_{met}_{sun}"
                col_zone = f"Zone_{met}_{sun}"
                merged_df[[col_ehi, col_zone]] = merged_df.apply(
                    lambda row: calc_ehi_zone(row, met, sun), axis=1
                )
        print("✓ Calculated EHI for all conditions")

        # Define column order for emails and API
        final_cols = [
            "LOGGED_AT (IST)", "STATE", "DISTRICT", "STATION", "TEMP", "RH",
            "EHI (in shade °C)", "Light Work Heat Stress Zone",
            "EHI_350 (in shade °C)", "Hard Labor Heat Stress Zone"
        ]

        # Extended columns including all MET/sun combinations
        extended_cols = final_cols.copy()
        for met in [3, 4, 5, 6]:
            for sun in ['shade', 'sun']:
                extended_cols.extend([f"EHI_{met}_{sun}", f"Zone_{met}_{sun}"])

        # Filter rows where ZONE is 5 or 6 for alerts (Zone 4 is caution, not alert)
        alert_df = merged_df[merged_df["Hard Labor Heat Stress Zone"].isin(["Zone 5", "Zone 6"])]

        # Email sending disabled - using send_alerts.py instead (via update-grid-data.yml workflow)
        # if not alert_df.empty:
        #     msg = EmailMessage()
        #     msg["Subject"] = "Extreme Heat Stress Alert: Zones 5 and/or 6 Detected"
        #     ... (email code removed)
                
    except Exception as e:
        print("Error fetching from IMD:", e)

    try:
        os.makedirs(os.path.join(ROOT_DIR, 'weather_logs'), exist_ok=True)
        file_path = get_weekly_filename()
    
        if not os.path.isfile(file_path):
            merged_df.to_csv(file_path, index=False)
            print(f"✓ Created new weekly log: {file_path}")
        else:
            merged_df.to_csv(file_path, mode='a', header=False, index=False)
            print(f"✓ Appended {len(merged_df)} rows to {file_path}")
    
    except Exception as e:
        print("Error writing log to CSV:", e)

    # NEW: Save latest alerts as JSON for API access
    try:
        # Build zone counts for all MET levels and sun conditions (all zones 1-6)
        zone_counts = {}
        for met in [3, 4, 5, 6]:
            for sun in ['shade', 'sun']:
                zone_col = f"Zone_{met}_{sun}"
                key_prefix = f"met{met}_{sun}"
                for zone_num in [1, 2, 3, 4, 5, 6]:
                    zone_counts[f"{key_prefix}_zone{zone_num}"] = len(merged_df[merged_df[zone_col] == f"Zone {zone_num}"])

        # Include all EHI columns in output
        all_cols = final_cols.copy()
        for met in [3, 4, 5, 6]:
            for sun in ['shade', 'sun']:
                all_cols.extend([f"EHI_{met}_{sun}", f"Zone_{met}_{sun}"])

        # Filter ALERTS based only on Zone 5 and 6 (not Zone 4 - that's just caution)
        alert_conditions = pd.Series([False] * len(merged_df))
        for met in [3, 4, 5, 6]:
            for sun in ['shade', 'sun']:
                zone_col = f"Zone_{met}_{sun}"
                alert_conditions |= merged_df[zone_col].isin(["Zone 5", "Zone 6"])

        alerts_df = merged_df[alert_conditions]

        # Check if nighttime (6 PM to 6 AM IST) - no sun exposure at night
        now_ist = datetime.now(ZoneInfo("Asia/Calcutta"))
        current_hour = now_ist.hour
        is_nighttime = current_hour >= 18 or current_hour < 6

        # Replace NaN with None for valid JSON (NaN is not valid JSON)
        # Using .replace({np.nan: None}) instead of .where() for proper JSON serialization
        alerts_data = alerts_df[all_cols].replace({np.nan: None}).to_dict(orient='records') if not alerts_df.empty else []

        alerts_json = {
            "timestamp": datetime.now(ZoneInfo("Asia/Calcutta")).isoformat(),
            "total_stations": len(merged_df),
            "alert_count": len(alerts_df),  # Only Zone 5 and 6 count as alerts
            "zone_counts": zone_counts,
            "is_nighttime": is_nighttime,  # True if 6 PM - 6 AM IST (no sun values)
            # Legacy fields for backward compatibility (EHI-6 shade)
            "zone_6_count": zone_counts.get("met6_shade_zone6", 0),
            "zone_5_count": zone_counts.get("met6_shade_zone5", 0),
            "zone_4_count": zone_counts.get("met6_shade_zone4", 0),
            "alerts": alerts_data
        }

        with open(os.path.join(ROOT_DIR, 'weather_logs', 'latest_alerts.json'), 'w') as f:
            json.dump(alerts_json, f, indent=2)
        print(f"✓ Saved {len(alerts_df)} alerts to JSON (Zone 5 & 6 only)")

    except Exception as e:
        print("Error saving alerts JSON:", e)

    # NEW: Save last 24 hours of data (all zones 1-6)
    try:
        # Load existing history
        history_file = os.path.join(ROOT_DIR, 'weather_logs', 'alerts_24h.json')
        if os.path.exists(history_file):
            with open(history_file, 'r') as f:
                history = json.load(f)
            # Handle old format with "hourly_alerts" key
            if "hourly_alerts" in history and "hourly_data" not in history:
                history["hourly_data"] = history.pop("hourly_alerts")
        else:
            history = {"hourly_data": []}

        # Replace NaN values with None for valid JSON (NaN is not valid JSON)
        # Using .replace({np.nan: None}) instead of .where() for proper JSON serialization
        stations_df = merged_df[all_cols].copy() if 'all_cols' in dir() else pd.DataFrame()
        stations_data = stations_df.replace({np.nan: None}).to_dict(orient='records') if not stations_df.empty else []

        # Add current hour's data - includes ALL stations with all zones (1-6)
        current_entry = {
            "timestamp": datetime.now(ZoneInfo("Asia/Calcutta")).isoformat(),
            "total_stations": len(merged_df),
            "alert_count": len(alerts_df) if 'alerts_df' in dir() and not alerts_df.empty else 0,  # Only Zone 5 & 6
            "zone_counts": zone_counts if 'zone_counts' in dir() else {},  # All zones 1-6
            "stations": stations_data  # All stations with NaN replaced by null
        }

        history["hourly_data"].insert(0, current_entry)  # Add to front

        # Keep only last 24 hours
        history["hourly_data"] = history["hourly_data"][:24]

        # Save updated history
        with open(history_file, 'w') as f:
            json.dump(history, f, indent=2)

        print(f"✓ Saved 24h history ({len(history['hourly_data'])} hours, all zones)")
    except Exception as e:
        print("Error saving 24h history:", e)

    # COMMENTED OUT: Summer statistics feature disabled
    # To re-enable, uncomment the block below
    """
    # NEW: Calculate and save summer statistics
    try:
        now = datetime.now(ZoneInfo("Asia/Calcutta"))
        current_year = now.year
        is_summer = now.month in [3, 4, 5, 6, 7, 8, 9]  # March-September

        stats_file = os.path.join(ROOT_DIR, 'weather_logs', 'summer_stats.json')

        # Initialize zone events structure for all MET/sun combinations (all zones 1-6)
        def init_zone_events():
            events = {}
            for met in [3, 4, 5, 6]:
                for sun in ['shade', 'sun']:
                    key = f"met{met}_{sun}"
                    for zone_num in [1, 2, 3, 4, 5, 6]:
                        events[f"{key}_zone{zone_num}"] = 0
            return events

        # Load existing stats or create new
        if os.path.exists(stats_file):
            with open(stats_file, 'r') as f:
                summer_stats = json.load(f)

            # Reset if it's a new year
            if summer_stats.get("year") != current_year:
                print(f"New year detected - resetting summer stats for {current_year}")
                summer_stats = {
                    "year": current_year,
                    "season_start": f"{current_year}-06-01",
                    "total_hourly_checks": 0,
                    "total_alerts": 0,
                    "zone_events": init_zone_events(),
                    # Legacy fields for backward compatibility
                    "zone_6_events": 0,
                    "zone_5_events": 0,
                    "zone_4_events": 0,
                    "peak_temp": None,
                    "peak_ehi350": None,
                    "hottest_location": None,
                    "last_updated": None,
                    "data_collection_started": now.isoformat()
                }
        else:
            # First time creating stats file
            print(f"Creating new summer statistics file for {current_year}")
            summer_stats = {
                "year": current_year,
                "season_start": f"{current_year}-06-01" if is_summer else f"{current_year + 1}-06-01",
                "total_hourly_checks": 0,
                "total_alerts": 0,
                "zone_events": init_zone_events(),
                # Legacy fields for backward compatibility
                "zone_6_events": 0,
                "zone_5_events": 0,
                "zone_4_events": 0,
                "peak_temp": None,
                "peak_ehi350": None,
                "hottest_location": None,
                "last_updated": None,
                "data_collection_started": now.isoformat()
            }

        # Ensure zone_events exists (for migration from old format)
        if "zone_events" not in summer_stats:
            summer_stats["zone_events"] = init_zone_events()

        # Only accumulate stats during summer months (or always if you want year-round tracking)
        if is_summer:
            summer_stats["total_hourly_checks"] += 1

            # Update zone events for all MET levels and sun conditions
            if 'zone_counts' in dir():
                for key, count in zone_counts.items():
                    if key in summer_stats["zone_events"]:
                        summer_stats["zone_events"][key] += count

            # Legacy fields (EHI-6 shade) - only Zone 5 & 6 count as alerts
            if not alerts_df.empty:
                summer_stats["total_alerts"] += len(alerts_df)  # Only Zone 5 & 6
                summer_stats["zone_6_events"] += len(alerts_df[alerts_df["Hard Labor Heat Stress Zone"] == "Zone 6"])
                summer_stats["zone_5_events"] += len(alerts_df[alerts_df["Hard Labor Heat Stress Zone"] == "Zone 5"])
            # Zone 4 events tracked separately (not counted as alerts)
            zone4_count = len(merged_df[merged_df["Hard Labor Heat Stress Zone"] == "Zone 4"])
            summer_stats["zone_4_events"] += zone4_count

            # Track peak values
            if not merged_df.empty and not pd.isna(merged_df["TEMP"].max()):
                current_max_temp = float(merged_df["TEMP"].max())
                if summer_stats["peak_temp"] is None or current_max_temp > summer_stats["peak_temp"]:
                    summer_stats["peak_temp"] = current_max_temp
                    hottest_row = merged_df.loc[merged_df["TEMP"].idxmax()]
                    summer_stats["hottest_location"] = {
                        "station": hottest_row["STATION"],
                        "district": hottest_row["DISTRICT"],
                        "state": hottest_row["STATE"],
                        "temp": current_max_temp,
                        "date": now.strftime("%Y-%m-%d %H:%M:%S")
                    }

            if not merged_df.empty and not pd.isna(merged_df["EHI_350 (in shade °C)"].max()):
                current_max_ehi = float(merged_df["EHI_350 (in shade °C)"].max())
                if summer_stats["peak_ehi350"] is None or current_max_ehi > summer_stats["peak_ehi350"]:
                    summer_stats["peak_ehi350"] = current_max_ehi
                    ehi_row = merged_df.loc[merged_df["EHI_350 (in shade °C)"].idxmax()]
                    summer_stats["peak_ehi350_location"] = {
                        "station": ehi_row["STATION"],
                        "district": ehi_row["DISTRICT"],
                        "state": ehi_row["STATE"],
                        "ehi350": current_max_ehi,
                        "date": now.strftime("%Y-%m-%d %H:%M:%S")
                    }
        
        summer_stats["last_updated"] = now.isoformat()
        
        with open(stats_file, 'w') as f:
            json.dump(summer_stats, f, indent=2)
        
        if is_summer:
            print(f"✓ Updated summer {current_year} statistics")
        else:
            print(f"✓ Outside summer season - stats saved but not accumulating")
            
    except Exception as e:
        print("Error saving summer stats:", e)
    """
    # END OF COMMENTED OUT SUMMER STATS

fetch_and_log()
 