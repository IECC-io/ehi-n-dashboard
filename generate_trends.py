"""
Generate Zone Trend Data (Daily, Weekly, Monthly)
=================================================

Aggregates hourly weather CSV data into trend summaries for the dashboard.
Outputs JSON files that can be loaded by the frontend.

Outputs:
- weather_logs/trends_daily.json   - Last 7 days, hourly data points
- weather_logs/trends_weekly.json  - Last 4 weeks, daily averages
- weather_logs/trends_monthly.json - Last 6 months, weekly averages
"""

import os
import json
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from glob import glob

# Configuration
WEATHER_LOGS_DIR = "weather_logs"
IST = ZoneInfo("Asia/Calcutta")

# Zone columns for each MET level and sun/shade
ZONE_COLS = {
    'met3_shade': 'Zone_3_shade',
    'met3_sun': 'Zone_3_sun',
    'met4_shade': 'Zone_4_shade',
    'met4_sun': 'Zone_4_sun',
    'met5_shade': 'Zone_5_shade',
    'met5_sun': 'Zone_5_sun',
    'met6_shade': 'Zone_6_shade',
    'met6_sun': 'Zone_6_sun',
}


def load_csv_files(start_date, end_date=None):
    """Load CSV files covering the date range."""
    if end_date is None:
        end_date = datetime.now(IST)

    # Convert to timezone-naive for comparison with pandas timestamps
    start_naive = start_date.replace(tzinfo=None) if hasattr(start_date, 'tzinfo') and start_date.tzinfo else start_date
    end_naive = end_date.replace(tzinfo=None) if hasattr(end_date, 'tzinfo') and end_date.tzinfo else end_date

    # Find all relevant weekly CSV files
    csv_files = sorted(glob(f"{WEATHER_LOGS_DIR}/india_weather_*.csv"))

    dfs = []
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file, low_memory=False, on_bad_lines='skip')
            if 'LOGGED_AT (IST)' in df.columns:
                df['timestamp'] = pd.to_datetime(df['LOGGED_AT (IST)'], errors='coerce')
                df = df.dropna(subset=['timestamp'])
                # Filter to date range
                df = df[(df['timestamp'] >= start_naive) & (df['timestamp'] <= end_naive)]
                if len(df) > 0:
                    dfs.append(df)
        except Exception as e:
            print(f"  Warning: Could not load {csv_file}: {e}")

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()


def count_zones(df, zone_col):
    """Count districts in each zone (1-6) for a given zone column."""
    counts = {f'zone{i}': 0 for i in range(1, 7)}

    if zone_col not in df.columns:
        return counts

    for zone in range(1, 7):
        zone_str = f'Zone {zone}'
        counts[f'zone{zone}'] = int((df[zone_col] == zone_str).sum())

    return counts


def aggregate_hourly(df):
    """Aggregate data by hour, counting zones for each MET/sun combo."""
    if df.empty:
        return []

    # Round to hour
    df['hour'] = df['timestamp'].dt.floor('h')

    results = []
    for hour, group in df.groupby('hour'):
        entry = {
            'timestamp': hour.isoformat(),
            'total_stations': len(group),
        }

        # Count zones for each MET/sun combination
        for key, col in ZONE_COLS.items():
            entry[key] = count_zones(group, col)

        # Add temp/RH averages
        if 'TEMP' in group.columns:
            temp_vals = pd.to_numeric(group['TEMP'], errors='coerce').dropna()
            if len(temp_vals) > 0:
                entry['avg_temp'] = round(float(temp_vals.mean()), 1)
                entry['min_temp'] = round(float(temp_vals.min()), 1)
                entry['max_temp'] = round(float(temp_vals.max()), 1)

        if 'RH' in group.columns:
            rh_vals = pd.to_numeric(group['RH'], errors='coerce').dropna()
            if len(rh_vals) > 0:
                entry['avg_rh'] = round(float(rh_vals.mean()), 1)
                entry['min_rh'] = round(float(rh_vals.min()), 1)
                entry['max_rh'] = round(float(rh_vals.max()), 1)

        results.append(entry)

    return sorted(results, key=lambda x: x['timestamp'])


def aggregate_daily(df):
    """Aggregate data by day, averaging zone counts and temp/RH."""
    if df.empty:
        return []

    df['date'] = df['timestamp'].dt.date

    results = []
    for date, group in df.groupby('date'):
        # Get hourly aggregates for this day
        hourly = aggregate_hourly(group)

        if not hourly:
            continue

        # Average the zone counts across hours
        entry = {
            'date': str(date),
            'hours_recorded': len(hourly),
        }

        for key in ZONE_COLS.keys():
            zone_totals = {f'zone{i}': 0 for i in range(1, 7)}
            for h in hourly:
                if key in h:
                    for z in range(1, 7):
                        zone_totals[f'zone{z}'] += h[key].get(f'zone{z}', 0)

            # Average
            entry[key] = {
                f'zone{i}': round(zone_totals[f'zone{i}'] / len(hourly))
                for i in range(1, 7)
            }

        # Average temp/RH across hours
        temps = [h['avg_temp'] for h in hourly if 'avg_temp' in h]
        if temps:
            entry['avg_temp'] = round(sum(temps) / len(temps), 1)
            entry['min_temp'] = round(min(h['min_temp'] for h in hourly if 'min_temp' in h), 1)
            entry['max_temp'] = round(max(h['max_temp'] for h in hourly if 'max_temp' in h), 1)

        rhs = [h['avg_rh'] for h in hourly if 'avg_rh' in h]
        if rhs:
            entry['avg_rh'] = round(sum(rhs) / len(rhs), 1)
            entry['min_rh'] = round(min(h['min_rh'] for h in hourly if 'min_rh' in h), 1)
            entry['max_rh'] = round(max(h['max_rh'] for h in hourly if 'max_rh' in h), 1)

        results.append(entry)

    return sorted(results, key=lambda x: x['date'])


def aggregate_weekly(daily_data):
    """Aggregate daily data into weekly summaries."""
    if not daily_data:
        return []

    # Group by ISO week
    weekly = {}
    for day in daily_data:
        date = datetime.strptime(day['date'], '%Y-%m-%d')
        week_key = date.strftime('%Y-W%W')

        if week_key not in weekly:
            weekly[week_key] = {
                'week': week_key,
                'start_date': day['date'],
                'days': []
            }

        weekly[week_key]['days'].append(day)
        weekly[week_key]['end_date'] = day['date']

    # Average across days in each week
    results = []
    for week_key, week_data in weekly.items():
        days = week_data['days']
        entry = {
            'week': week_key,
            'start_date': week_data['start_date'],
            'end_date': week_data['end_date'],
            'days_recorded': len(days),
        }

        for key in ZONE_COLS.keys():
            zone_totals = {f'zone{i}': 0 for i in range(1, 7)}
            for d in days:
                if key in d:
                    for z in range(1, 7):
                        zone_totals[f'zone{z}'] += d[key].get(f'zone{z}', 0)

            entry[key] = {
                f'zone{i}': round(zone_totals[f'zone{i}'] / len(days))
                for i in range(1, 7)
            }

        # Average temp/RH across days
        temps = [d['avg_temp'] for d in days if 'avg_temp' in d]
        if temps:
            entry['avg_temp'] = round(sum(temps) / len(temps), 1)
            entry['min_temp'] = round(min(d['min_temp'] for d in days if 'min_temp' in d), 1)
            entry['max_temp'] = round(max(d['max_temp'] for d in days if 'max_temp' in d), 1)

        rhs = [d['avg_rh'] for d in days if 'avg_rh' in d]
        if rhs:
            entry['avg_rh'] = round(sum(rhs) / len(rhs), 1)
            entry['min_rh'] = round(min(d['min_rh'] for d in days if 'min_rh' in d), 1)
            entry['max_rh'] = round(max(d['max_rh'] for d in days if 'max_rh' in d), 1)

        results.append(entry)

    return sorted(results, key=lambda x: x['week'])


def generate_daily_trends():
    """Generate last 7 days of hourly data."""
    print("Generating daily trends (last 7 days, hourly)...")

    end_date = datetime.now(IST)
    start_date = end_date - timedelta(days=7)

    df = load_csv_files(start_date, end_date)
    print(f"  Loaded {len(df)} records")

    hourly_data = aggregate_hourly(df)
    print(f"  Generated {len(hourly_data)} hourly data points")

    output = {
        'generated': datetime.now(IST).isoformat(),
        'period': 'daily',
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'data': hourly_data
    }

    output_file = f"{WEATHER_LOGS_DIR}/trends_daily.json"
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"  Saved to {output_file}")
    return output


def generate_weekly_trends():
    """Generate last 4 weeks of daily averages."""
    print("Generating weekly trends (last 4 weeks, daily)...")

    end_date = datetime.now(IST)
    start_date = end_date - timedelta(weeks=4)

    df = load_csv_files(start_date, end_date)
    print(f"  Loaded {len(df)} records")

    daily_data = aggregate_daily(df)
    print(f"  Generated {len(daily_data)} daily data points")

    output = {
        'generated': datetime.now(IST).isoformat(),
        'period': 'weekly',
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'data': daily_data
    }

    output_file = f"{WEATHER_LOGS_DIR}/trends_weekly.json"
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"  Saved to {output_file}")
    return output


def generate_monthly_trends():
    """Generate last 6 months of weekly averages."""
    print("Generating monthly trends (last 6 months, weekly)...")

    end_date = datetime.now(IST)
    start_date = end_date - timedelta(days=180)  # ~6 months

    df = load_csv_files(start_date, end_date)
    print(f"  Loaded {len(df)} records")

    daily_data = aggregate_daily(df)
    weekly_data = aggregate_weekly(daily_data)
    print(f"  Generated {len(weekly_data)} weekly data points")

    output = {
        'generated': datetime.now(IST).isoformat(),
        'period': 'monthly',
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'data': weekly_data
    }

    output_file = f"{WEATHER_LOGS_DIR}/trends_monthly.json"
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"  Saved to {output_file}")
    return output


def main():
    print("=" * 60)
    print("GENERATING ZONE TREND DATA")
    print("=" * 60)

    os.makedirs(WEATHER_LOGS_DIR, exist_ok=True)

    generate_daily_trends()
    generate_weekly_trends()
    generate_monthly_trends()

    print("\n" + "=" * 60)
    print("DONE!")
    print("=" * 60)


if __name__ == '__main__':
    main()
