#!/usr/bin/env python3
"""
Generate pre-computed 3-day forecast data for all district capitals in India.
Fetches weather forecast from WeatherAPI.com and computes EHI/zones using lookup tables.
Run this daily at 5pm IST via GitHub Actions.
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

# WeatherAPI.com API key
WEATHER_API_KEY = '4753e967970b4abca6b63520261401'

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


def fetch_forecast(lat, lon, days=3):
    """Fetch forecast data from WeatherAPI.com."""
    url = f'https://api.weatherapi.com/v1/forecast.json?key={WEATHER_API_KEY}&q={lat},{lon}&days={days}&aqi=no'

    try:
        response = requests.get(url, timeout=30)
        data = response.json()

        if 'forecast' in data:
            return data
        elif 'error' in data:
            print(f"API error for ({lat}, {lon}): {data['error'].get('message', 'Unknown error')}")
            return None
        else:
            return None
    except Exception as e:
        print(f"Error fetching forecast ({lat}, {lon}): {e}")
        return None


def process_forecast_day(day_data, met_levels=[3, 4, 5, 6]):
    """Process a single forecast day and compute EHI for all hours."""
    hours = []

    for hour_data in day_data.get('hour', []):
        hour_info = {
            'time': hour_data['time'],
            'temp_c': hour_data['temp_c'],
            'humidity': hour_data['humidity'],
            'condition': hour_data['condition']['text'],
            'data': {}
        }

        # Compute EHI for all MET levels and sun conditions
        for met in met_levels:
            hour_info['data'][f'met{met}'] = {}
            for sun in ['shade', 'sun']:
                ehi, zone = compute_ehi_and_zone(
                    hour_data['temp_c'],
                    hour_data['humidity'],
                    met,
                    sun
                )
                hour_info['data'][f'met{met}'][sun] = {
                    'ehi': round(ehi, 1) if ehi is not None else None,
                    'zone': zone
                }

        hours.append(hour_info)

    return {
        'date': day_data['date'],
        'day': {
            'maxtemp_c': day_data['day']['maxtemp_c'],
            'mintemp_c': day_data['day']['mintemp_c'],
            'avgtemp_c': day_data['day']['avgtemp_c'],
            'avghumidity': day_data['day']['avghumidity'],
            'condition': day_data['day']['condition']['text']
        },
        'hours': hours
    }


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
            'sun_conditions': ['shade', 'sun']
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
            forecast_data = fetch_forecast(capital['lat'], capital['lon'])
            if forecast_data and 'forecast' in forecast_data:
                state_output['capital'] = {
                    'name': capital.get('name', state_name),
                    'lat': capital['lat'],
                    'lon': capital['lon'],
                    'forecast': [
                        process_forecast_day(day)
                        for day in forecast_data['forecast']['forecastday']
                    ]
                }
                processed += 1
            else:
                errors += 1
            time.sleep(0.2)  # Rate limiting

        # Fetch forecast for each district
        for district_name, district_coords in state_data.get('districts', {}).items():
            if district_coords.get('lat') and district_coords.get('lon'):
                forecast_data = fetch_forecast(district_coords['lat'], district_coords['lon'])
                if forecast_data and 'forecast' in forecast_data:
                    state_output['districts'][district_name] = {
                        'lat': district_coords['lat'],
                        'lon': district_coords['lon'],
                        'forecast': [
                            process_forecast_day(day)
                            for day in forecast_data['forecast']['forecastday']
                        ]
                    }
                    processed += 1
                else:
                    errors += 1
                time.sleep(0.2)  # Rate limiting

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
