#!/usr/bin/env python3
"""
Recalculate EHI zones for all historical CSV files using updated lookup tables.

This script reads each weekly CSV file and recalculates the zone columns
using the new lookup tables that properly differentiate Zone 2 and Zone 3.
"""

import os
import pandas as pd
from glob import glob
from ehi_lookup import EHILookup

# Path setup
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)  # Parent of scripts/

# Initialize lookup
ehi_lookup = EHILookup()

# Zone mapping including Zone 3
ZONE_MAP = {1: "Zone 1", 2: "Zone 2", 3: "Zone 3", 4: "Zone 4", 5: "Zone 5", 6: "Zone 6"}

def recalc_ehi_zone(row, met_level, sun_condition):
    """Recalculate EHI and zone for a row."""
    try:
        temp_c = row["TEMP"] if pd.notna(row["TEMP"]) else None
        rh_percent = row["RH"] if pd.notna(row["RH"]) else None
        if temp_c is not None and rh_percent is not None:
            ehi, zone_num = ehi_lookup.get_ehi_zone(temp_c, rh_percent, met_level, sun_condition)
            zone = ZONE_MAP.get(zone_num, "Unknown")
            return pd.Series([ehi, zone])
        else:
            return pd.Series([None, None])
    except Exception as e:
        return pd.Series([None, None])

def recalculate_file(filepath):
    """Recalculate zones for a single CSV file."""
    print(f"Processing: {os.path.basename(filepath)}", flush=True)

    try:
        df = pd.read_csv(filepath, low_memory=False, on_bad_lines='skip')
        original_len = len(df)

        # Check if this file has TEMP and RH columns
        if 'TEMP' not in df.columns or 'RH' not in df.columns:
            print(f"  Skipping - no TEMP/RH columns found")
            return False

        # Recalculate all MET/sun combinations
        print(f"  Recalculating zones for {original_len} rows...", flush=True)
        for met in [3, 4, 5, 6]:
            for sun in ['shade', 'sun']:
                col_ehi = f"EHI_{met}_{sun}"
                col_zone = f"Zone_{met}_{sun}"

                # Always calculate - create columns if they don't exist
                df[[col_ehi, col_zone]] = df.apply(
                    lambda row: recalc_ehi_zone(row, met, sun), axis=1
                )

        # Also calculate the legacy columns
        df[["EHI_350 (in shade °C)", "Hard Labor Heat Stress Zone"]] = df.apply(
            lambda row: recalc_ehi_zone(row, 6, 'shade'), axis=1
        )

        # Save back
        df.to_csv(filepath, index=False)

        # Count zone distribution
        zone_cols = [col for col in df.columns if col.startswith('Zone_')]
        zone_counts = {}
        for col in zone_cols:
            counts = df[col].value_counts()
            for zone, count in counts.items():
                if zone and "Zone" in str(zone):
                    zone_counts[zone] = zone_counts.get(zone, 0) + count

        print(f"  Done. Zone distribution sample:", flush=True)
        for zone in sorted(zone_counts.keys()):
            print(f"    {zone}: {zone_counts[zone]}", flush=True)

        return True

    except Exception as e:
        print(f"  ERROR: {e}")
        return False

def main():
    print("=" * 60)
    print("RECALCULATING ZONES WITH UPDATED LOOKUP TABLES")
    print("=" * 60)

    # Find all weekly CSV files
    weather_logs_dir = os.path.join(ROOT_DIR, 'weather_logs')
    csv_files = sorted(glob(os.path.join(weather_logs_dir, "india_weather_*.csv")))
    print(f"\nFound {len(csv_files)} CSV files to process")

    success = 0
    failed = 0

    for filepath in csv_files:
        if recalculate_file(filepath):
            success += 1
        else:
            failed += 1

    print("\n" + "=" * 60)
    print(f"DONE! Processed {success} files, {failed} skipped/failed")
    print("=" * 60)

if __name__ == '__main__':
    main()
