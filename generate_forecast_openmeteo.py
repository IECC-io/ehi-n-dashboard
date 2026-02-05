#!/usr/bin/env python3
"""
Generate pre-computed 3-day forecast data for all district capitals in India.
Fetches weather forecast from Open-Meteo API and computes EHI/zones using lookup tables.
Run this daily at 5pm IST via GitHub Actions.

Open-Meteo API: https://open-meteo.com/
- Free tier: Unlimited for non-commercial use
- Customer API: 1M calls/month with apikey
"""

import json
import requests
from datetime import datetime
import pytz
import os
import time

# Use lookup tables for EHI calculations (no NumbaMinpack/scipy needed)
from ehi_lookup import EHILookup

print("Using EHI lookup tables")

# Initialize lookup
lookup = EHILookup()

# Open-Meteo API configuration
OPENMETEO_API_KEY = 'lTfTNEkGgK34jXrq'
OPENMETEO_BASE_URL = 'https://customer-api.open-meteo.com/v1/forecast'

# MET levels in W/m²
MET_LEVELS = {
    3: 180,   # Light work
    4: 240,   # Moderate work
    5: 300,   # Heavy work
    6: 360    # Very heavy work
}


def compute_ehi_and_zone(temp_c, rh_percent, met_level, sun_condition):
    """Compute EHI and zone for given conditions using lookup tables."""
    if temp_c is None or rh_percent is None:
        return None, 0

    try:
        ehi, zone = lookup.get_ehi_zone(temp_c, rh_percent, met_level, sun_condition)
        return ehi, zone
    except Exception as e:
        print(f"Error looking up EHI: {e}")
        return None, 0


def fetch_forecast_openmeteo(lat, lon, days=3):
    """Fetch forecast data from Open-Meteo API."""
    url = (
        f"{OPENMETEO_BASE_URL}"
        f"?latitude={lat}"
        f"&longitude={lon}"
        f"&hourly=temperature_2m,relative_humidity_2m,weather_code"
        f"&daily=temperature_2m_max,temperature_2m_min,weather_code"
        f"&timezone=Asia/Kolkata"
        f"&forecast_days={days}"
        f"&apikey={OPENMETEO_API_KEY}"
    )

    try:
        response = requests.get(url, timeout=30)
        data = response.json()

        if 'hourly' in data:
            return data
        elif 'error' in data:
            print(f"API error for ({lat}, {lon}): {data.get('reason', 'Unknown error')}")
            return None
        else:
            return None
    except Exception as e:
        print(f"Error fetching forecast ({lat}, {lon}): {e}")
        return None


def weather_code_to_condition(code):
    """Convert Open-Meteo weather code to human-readable condition."""
    # https://open-meteo.com/en/docs#weathervariables
    conditions = {
        0: 'Clear sky',
        1: 'Mainly clear',
        2: 'Partly cloudy',
        3: 'Overcast',
        45: 'Fog',
        48: 'Depositing rime fog',
        51: 'Light drizzle',
        53: 'Moderate drizzle',
        55: 'Dense drizzle',
        61: 'Slight rain',
        63: 'Moderate rain',
        65: 'Heavy rain',
        71: 'Slight snow',
        73: 'Moderate snow',
        75: 'Heavy snow',
        77: 'Snow grains',
        80: 'Slight rain showers',
        81: 'Moderate rain showers',
        82: 'Violent rain showers',
        85: 'Slight snow showers',
        86: 'Heavy snow showers',
        95: 'Thunderstorm',
        96: 'Thunderstorm with slight hail',
        99: 'Thunderstorm with heavy hail',
    }
    return conditions.get(code, 'Unknown')


def process_forecast_data(data, met_levels=[3, 4, 5, 6]):
    """Process Open-Meteo forecast data and compute EHI for all hours."""
    hourly = data.get('hourly', {})
    daily = data.get('daily', {})

    times = hourly.get('time', [])
    temps = hourly.get('temperature_2m', [])
    humidities = hourly.get('relative_humidity_2m', [])
    weather_codes = hourly.get('weather_code', [])

    # Group hours by date
    days_data = {}
    for i, time_str in enumerate(times):
        date = time_str.split('T')[0]
        if date not in days_data:
            days_data[date] = {
                'date': date,
                'hours': []
            }

        # Extract hour from time string to determine day/night
        hour_of_day = int(time_str.split('T')[1].split(':')[0])
        is_nighttime = hour_of_day >= 18 or hour_of_day < 6

        hour_info = {
            'time': time_str.replace('T', ' '),
            'temp_c': temps[i] if i < len(temps) else None,
            'humidity': humidities[i] if i < len(humidities) else None,
            'condition': weather_code_to_condition(weather_codes[i]) if i < len(weather_codes) else 'Unknown',
            'is_night': is_nighttime,
            'data': {}
        }

        # Compute EHI for all MET levels and sun conditions
        for met in met_levels:
            hour_info['data'][f'met{met}'] = {}

            # Always compute shade first
            shade_ehi, shade_zone = compute_ehi_and_zone(
                hour_info['temp_c'],
                hour_info['humidity'],
                met,
                'shade'
            )
            hour_info['data'][f'met{met}']['shade'] = {
                'ehi': round(shade_ehi, 1) if shade_ehi is not None else None,
                'zone': shade_zone
            }

            # For sun: if nighttime, use shade values; otherwise compute normally
            if is_nighttime:
                hour_info['data'][f'met{met}']['sun'] = {
                    'ehi': round(shade_ehi, 1) if shade_ehi is not None else None,
                    'zone': shade_zone
                }
            else:
                sun_ehi, sun_zone = compute_ehi_and_zone(
                    hour_info['temp_c'],
                    hour_info['humidity'],
                    met,
                    'sun'
                )
                hour_info['data'][f'met{met}']['sun'] = {
                    'ehi': round(sun_ehi, 1) if sun_ehi is not None else None,
                    'zone': sun_zone
                }

        days_data[date]['hours'].append(hour_info)

    # Add daily summary data
    daily_dates = daily.get('time', [])
    daily_max = daily.get('temperature_2m_max', [])
    daily_min = daily.get('temperature_2m_min', [])
    daily_codes = daily.get('weather_code', [])

    result = []
    for i, date in enumerate(sorted(days_data.keys())):
        day_data = days_data[date]

        # Find matching daily data
        daily_idx = daily_dates.index(date) if date in daily_dates else -1

        day_data['day'] = {
            'maxtemp_c': daily_max[daily_idx] if daily_idx >= 0 and daily_idx < len(daily_max) else None,
            'mintemp_c': daily_min[daily_idx] if daily_idx >= 0 and daily_idx < len(daily_min) else None,
            'avgtemp_c': None,  # Compute from hourly
            'avghumidity': None,  # Compute from hourly
            'condition': weather_code_to_condition(daily_codes[daily_idx]) if daily_idx >= 0 and daily_idx < len(daily_codes) else 'Unknown'
        }

        # Compute averages from hourly data
        temps_for_day = [h['temp_c'] for h in day_data['hours'] if h['temp_c'] is not None]
        humidities_for_day = [h['humidity'] for h in day_data['hours'] if h['humidity'] is not None]

        if temps_for_day:
            day_data['day']['avgtemp_c'] = round(sum(temps_for_day) / len(temps_for_day), 1)
        if humidities_for_day:
            day_data['day']['avghumidity'] = round(sum(humidities_for_day) / len(humidities_for_day), 0)

        result.append(day_data)

    return result


def generate_forecasts():
    """Generate forecasts for all district capitals."""

    # Load districts data
    print("Loading districts data...")
    with open('india_districts.json', 'r') as f:
        districts_data = json.load(f)

    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)

    output = {
        'metadata': {
            'generated_at': now.isoformat(),
            'generated_at_ist': now.strftime('%d %b %Y, %I:%M %p IST'),
            'forecast_days': 3,
            'met_levels': [3, 4, 5, 6],
            'sun_conditions': ['shade', 'sun'],
            'data_source': 'Open-Meteo'
        },
        'states': {}
    }

    total_locations = 0
    processed = 0
    errors = 0

    # Count total locations
    for state_name, state_data in districts_data['states'].items():
        total_locations += 1  # State capital
        total_locations += len(state_data.get('districts', {}))

    print(f"Processing {total_locations} locations...")

    for state_name, state_data in districts_data['states'].items():
        print(f"\nProcessing {state_name}...")

        state_output = {
            'capital': None,
            'districts': {}
        }

        # Fetch forecast for state capital
        capital = state_data.get('capital', {})
        if capital.get('lat') and capital.get('lon'):
            forecast_data = fetch_forecast_openmeteo(capital['lat'], capital['lon'])
            if forecast_data:
                state_output['capital'] = {
                    'name': capital.get('name', state_name),
                    'lat': capital['lat'],
                    'lon': capital['lon'],
                    'forecast': process_forecast_data(forecast_data)
                }
                processed += 1
            else:
                errors += 1
            time.sleep(0.1)  # Small delay to be nice to the API

        # Fetch forecast for each district
        for district_name, district_coords in state_data.get('districts', {}).items():
            if district_coords.get('lat') and district_coords.get('lon'):
                forecast_data = fetch_forecast_openmeteo(district_coords['lat'], district_coords['lon'])
                if forecast_data:
                    state_output['districts'][district_name] = {
                        'lat': district_coords['lat'],
                        'lon': district_coords['lon'],
                        'forecast': process_forecast_data(forecast_data)
                    }
                    processed += 1
                else:
                    errors += 1
                time.sleep(0.1)  # Small delay

            if (processed + errors) % 50 == 0:
                print(f"  Progress: {processed + errors}/{total_locations} ({processed} success, {errors} errors)")

        output['states'][state_name] = state_output

    # Save output
    output_path = 'weather_logs/forecast_data.json'
    os.makedirs('weather_logs', exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(output, f)

    file_size = os.path.getsize(output_path) / (1024 * 1024)
    print(f"\n✓ Saved forecast data to {output_path}")
    print(f"  File size: {file_size:.2f} MB")
    print(f"  Processed: {processed}/{total_locations}")
    print(f"  Errors: {errors}")
    print(f"  Generated at: {output['metadata']['generated_at_ist']}")

    return output


if __name__ == '__main__':
    generate_forecasts()
