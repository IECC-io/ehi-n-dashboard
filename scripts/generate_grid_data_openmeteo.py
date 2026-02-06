#!/usr/bin/env python3
"""
Generate pre-computed EHI grid data for all of India using Open-Meteo API.
Fetches weather at 0.25° resolution and computes EHI/zones using lookup tables.
Run this every 30 minutes via cron to keep data fresh.

Open-Meteo API: https://open-meteo.com/
- Free tier: Unlimited for non-commercial use
- Customer API: 1M calls/month with apikey
"""

import json
import requests
from datetime import datetime
import pytz
import os
import glob as glob_module

# Use lookup tables for EHI calculations (no NumbaMinpack/scipy needed)
from ehi_lookup import EHILookup

print("Using EHI lookup tables")

# Initialize lookup
lookup = EHILookup()

# Open-Meteo API configuration
OPENMETEO_API_KEY = 'lTfTNEkGgK34jXrq'
OPENMETEO_BASE_URL = 'https://customer-api.open-meteo.com/v1/forecast'

# Grid configuration - 0.25° resolution for higher detail
GRID_CONFIG = {
    'lat_min': 7.0,
    'lat_max': 37.0,
    'lon_min': 68.0,
    'lon_max': 97.0,
    'resolution': 0.25  # 0.25° resolution
}

# MET levels in W/m² (converting from MET units where 1 MET ≈ 58 W/m²)
MET_LEVELS = {
    3: 180,   # Light work (~3 MET)
    4: 240,   # Moderate work (~4 MET)
    5: 300,   # Heavy work (~5 MET)
    6: 360    # Very heavy work (~6 MET)
}


def load_india_boundary():
    """Load India boundary from local GeoJSON (includes all territories including Ladakh, J&K)."""
    geojson_dir = os.path.join(os.path.dirname(__file__) or '.', '..', 'geojson')
    states_file = os.path.join(geojson_dir, 'india_states.geojson')

    try:
        with open(states_file, 'r') as f:
            data = json.load(f)

        # Return the entire GeoJSON as a pseudo-feature containing all state polygons
        # This ensures all states including Ladakh and J&K are included
        print(f"Loaded {len(data['features'])} state/UT boundaries from local GeoJSON")
        return data
    except Exception as e:
        print(f"Error loading india_states.geojson: {e}")
        print("Falling back to bounding box only (no boundary filtering)")
        return None


def load_district_geojsons():
    """Load all district GeoJSON files and build a lookup structure."""
    districts = []
    geojson_dir = os.path.join(os.path.dirname(__file__) or '.', '..', 'geojson')

    # Find all district GeoJSON files
    pattern = os.path.join(geojson_dir, '*_districts.geojson')
    files = glob_module.glob(pattern)

    print(f"Loading {len(files)} district GeoJSON files...")

    for filepath in files:
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)

            # Extract state name from filename (e.g., gujarat_districts.geojson -> Gujarat)
            filename = os.path.basename(filepath)
            state_name = filename.replace('_districts.geojson', '').replace('_', ' ').title()

            for feature in data.get('features', []):
                district_name = feature.get('properties', {}).get('name', 'Unknown')
                geometry = feature.get('geometry', {})

                if geometry:
                    districts.append({
                        'name': district_name,
                        'state': state_name,
                        'geometry': geometry
                    })
        except Exception as e:
            print(f"  Error loading {filepath}: {e}")

    print(f"  Loaded {len(districts)} districts")
    return districts


def find_district_for_point(lat, lon, districts):
    """Find which district contains a given point."""
    for district in districts:
        geometry = district['geometry']

        if geometry['type'] == 'Polygon':
            if point_in_polygon(lat, lon, geometry['coordinates'][0]):
                return district['name'], district['state']
        elif geometry['type'] == 'MultiPolygon':
            for polygon in geometry['coordinates']:
                if point_in_polygon(lat, lon, polygon[0]):
                    return district['name'], district['state']

    return None, None


def point_in_polygon(lat, lon, polygon):
    """Ray casting algorithm for point-in-polygon test."""
    inside = False
    n = len(polygon)
    j = n - 1

    for i in range(n):
        # GeoJSON: polygon[i][0] = lon, polygon[i][1] = lat
        xi, yi = polygon[i][0], polygon[i][1]  # xi=lon, yi=lat
        xj, yj = polygon[j][0], polygon[j][1]

        if ((yi > lat) != (yj > lat)) and \
           (lon < (xj - xi) * (lat - yi) / (yj - yi) + xi):
            inside = not inside
        j = i

    return inside


def is_point_in_india(lat, lon, india_boundary):
    """Check if point is within any Indian state/UT boundary."""
    if india_boundary is None:
        return True

    # india_boundary is now a GeoJSON FeatureCollection with all states
    for feature in india_boundary.get('features', []):
        geometry = feature.get('geometry', {})

        if geometry.get('type') == 'Polygon':
            if point_in_polygon(lat, lon, geometry['coordinates'][0]):
                return True
        elif geometry.get('type') == 'MultiPolygon':
            for polygon in geometry['coordinates']:
                if point_in_polygon(lat, lon, polygon[0]):
                    return True

    return False


def generate_grid_points(india_boundary):
    """Generate grid points within India."""
    points = []
    lat = GRID_CONFIG['lat_min']
    while lat <= GRID_CONFIG['lat_max']:
        lon = GRID_CONFIG['lon_min']
        while lon <= GRID_CONFIG['lon_max']:
            if is_point_in_india(lat, lon, india_boundary):
                points.append({'lat': round(lat, 2), 'lon': round(lon, 2)})
            lon += GRID_CONFIG['resolution']
        lat += GRID_CONFIG['resolution']
    return points


def fetch_weather_batch_openmeteo(points, batch_size=50, max_retries=3):
    """
    Fetch weather data from Open-Meteo API in batches.
    Open-Meteo supports up to 100 locations per request.
    Includes retry logic to handle transient API failures.
    """
    import time
    all_weather = []

    for i in range(0, len(points), batch_size):
        batch = points[i:i + batch_size]

        # Build comma-separated lat/lon strings
        lats = ','.join(str(p['lat']) for p in batch)
        lons = ','.join(str(p['lon']) for p in batch)

        url = (
            f"{OPENMETEO_BASE_URL}"
            f"?latitude={lats}"
            f"&longitude={lons}"
            f"&current=temperature_2m,relative_humidity_2m"
            f"&timezone=Asia/Kolkata"
            f"&apikey={OPENMETEO_API_KEY}"
        )

        batch_results = None
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=60)
                data = response.json()

                # Handle single vs multiple location response
                if isinstance(data, list):
                    # Multiple locations returned as array
                    batch_results = []
                    for j, loc_data in enumerate(data):
                        if 'current' in loc_data:
                            batch_results.append({
                                'temp': loc_data['current'].get('temperature_2m'),
                                'rh': loc_data['current'].get('relative_humidity_2m')
                            })
                        else:
                            batch_results.append(None)
                    break  # Success
                elif 'current' in data:
                    # Single location
                    batch_results = [{
                        'temp': data['current'].get('temperature_2m'),
                        'rh': data['current'].get('relative_humidity_2m')
                    }]
                    break  # Success
                else:
                    # Error or no data
                    if 'error' in data:
                        print(f"API error (attempt {attempt+1}): {data.get('reason', 'Unknown error')}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s

            except Exception as e:
                print(f"Error fetching batch {i//batch_size + 1} (attempt {attempt+1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff

        # If all retries failed, use None for this batch
        if batch_results is None:
            print(f"  WARNING: Batch {i//batch_size + 1} failed after {max_retries} retries")
            batch_results = [None] * len(batch)

        all_weather.extend(batch_results)

        # Progress update
        fetched = min(i + batch_size, len(points))
        print(f"Fetched {fetched}/{len(points)} points")

    return all_weather


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


def load_previous_data(max_age_minutes=45):
    """
    Load previous grid data to fill gaps when API fails.
    Only uses data if it's less than max_age_minutes old (default 45 min).
    """
    script_dir = os.path.dirname(__file__) or '.'
    prev_file = os.path.join(script_dir, 'grid_data.json')

    if os.path.exists(prev_file):
        try:
            # Check file age
            file_age_seconds = datetime.now().timestamp() - os.path.getmtime(prev_file)
            file_age_minutes = file_age_seconds / 60

            if file_age_minutes > max_age_minutes:
                print(f"Previous data is {file_age_minutes:.0f} min old (>{max_age_minutes} min) - not using for gap fill")
                return {}, None

            with open(prev_file, 'r') as f:
                data = json.load(f)

            # Build lookup by (lat, lon)
            prev_lookup = {}
            for point in data.get('points', []):
                key = (point['lat'], point['lon'])
                prev_lookup[key] = point

            generated_at = data.get('metadata', {}).get('generated_at_ist', 'unknown')
            print(f"Loaded {len(prev_lookup)} points from previous data ({file_age_minutes:.0f} min old)")
            return prev_lookup, generated_at
        except Exception as e:
            print(f"Could not load previous data: {e}")
    return {}, None


def generate_grid_data():
    """Main function to generate pre-computed grid data."""
    print("Loading India boundary...")
    india_boundary = load_india_boundary()

    print("Loading district boundaries...")
    districts = load_district_geojsons()

    print("Generating grid points...")
    points = generate_grid_points(india_boundary)
    print(f"Found {len(points)} points within India at {GRID_CONFIG['resolution']}° resolution")

    # Load previous data for gap filling (only if < 45 min old)
    print("Loading previous data for gap filling...")
    prev_data, prev_generated_at = load_previous_data(max_age_minutes=45)

    # Pre-compute district for each point
    print("Mapping points to districts...")
    point_districts = {}
    for i, point in enumerate(points):
        district_name, state_name = find_district_for_point(point['lat'], point['lon'], districts)
        point_districts[(point['lat'], point['lon'])] = (district_name, state_name)
        if (i + 1) % 500 == 0:
            print(f"  Mapped {i + 1}/{len(points)} points to districts")

    print("Fetching weather data from Open-Meteo...")
    weather_data = fetch_weather_batch_openmeteo(points)

    # Count failures and fill from previous data (only if recent)
    failed_count = sum(1 for w in weather_data if w is None)
    filled_count = 0
    if failed_count > 0 and prev_data:
        print(f"  {failed_count} points failed - attempting to fill from previous data...")
        for i, (point, weather) in enumerate(zip(points, weather_data)):
            if weather is None:
                key = (point['lat'], point['lon'])
                if key in prev_data:
                    # Use previous temp/rh
                    prev_point = prev_data[key]
                    weather_data[i] = {
                        'temp': prev_point.get('temp'),
                        'rh': prev_point.get('rh')
                    }
                    filled_count += 1
        print(f"  Filled {filled_count}/{failed_count} gaps from previous data")

    # Prepare output data structure
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)

    # Check if it's nighttime (6pm-6am IST) - no sun exposure at night
    current_hour = now.hour
    is_nighttime = current_hour >= 18 or current_hour < 6
    if is_nighttime:
        print(f"Nighttime detected ({now.strftime('%I:%M %p IST')}) - sun values will equal shade values")

    # Calculate data quality metrics
    final_failed = sum(1 for w in weather_data if w is None)
    data_quality = 'good' if final_failed == 0 else ('partial' if final_failed < 100 else 'degraded')

    output = {
        'metadata': {
            'generated_at': now.isoformat(),
            'generated_at_ist': now.strftime('%d %b %Y, %I:%M %p IST'),
            'point_count': len(points),
            'resolution_deg': GRID_CONFIG['resolution'],
            'met_levels': [3, 4, 5, 6],
            'sun_conditions': ['shade', 'sun'],
            'is_nighttime': is_nighttime,
            'data_source': 'Open-Meteo',
            'data_quality': data_quality,
            'api_failures': failed_count,
            'filled_from_previous': filled_count,
            'remaining_gaps': final_failed
        },
        'points': []
    }

    print("Computing EHI and zones for all conditions...")
    valid_count = 0
    for i, (point, weather) in enumerate(zip(points, weather_data)):
        if weather is None:
            continue

        # Get district and state for this point
        district_name, state_name = point_districts.get((point['lat'], point['lon']), (None, None))

        # Build location string from district/state
        if district_name and state_name:
            location = f"{district_name}, {state_name}"
        elif state_name:
            location = state_name
        elif district_name:
            location = district_name
        else:
            location = f"{point['lat']:.2f}°N, {point['lon']:.2f}°E"

        point_data = {
            'lat': point['lat'],
            'lon': point['lon'],
            'location': location,
            'district': district_name,
            'state': state_name,
            'temp': weather['temp'],
            'rh': weather['rh'],
            'data': {}
        }

        # Compute for all MET levels and sun conditions
        for met in [3, 4, 5, 6]:
            point_data['data'][f'met{met}'] = {}

            # Always compute shade first
            shade_ehi, shade_zone = compute_ehi_and_zone(weather['temp'], weather['rh'], met, 'shade')
            point_data['data'][f'met{met}']['shade'] = {
                'ehi': round(shade_ehi, 1) if shade_ehi is not None else None,
                'zone': shade_zone
            }

            # For sun: if nighttime, use shade values; otherwise compute normally
            if is_nighttime:
                # Nighttime: sun = shade (no solar radiation at night)
                point_data['data'][f'met{met}']['sun'] = {
                    'ehi': round(shade_ehi, 1) if shade_ehi is not None else None,
                    'zone': shade_zone
                }
            else:
                # Daytime: compute sun exposure normally
                sun_ehi, sun_zone = compute_ehi_and_zone(weather['temp'], weather['rh'], met, 'sun')
                point_data['data'][f'met{met}']['sun'] = {
                    'ehi': round(sun_ehi, 1) if sun_ehi is not None else None,
                    'zone': sun_zone
                }

        output['points'].append(point_data)
        valid_count += 1

        if (i + 1) % 500 == 0:
            print(f"Processed {i + 1}/{len(points)} points")

    # Validate: check for latitude gaps (excluding ocean areas in far south)
    output_lats = set(p['lat'] for p in output['points'])
    expected_lats = set()
    lat = GRID_CONFIG['lat_min']
    while lat <= GRID_CONFIG['lat_max']:
        expected_lats.add(round(lat, 2))
        lat += GRID_CONFIG['resolution']

    # Check for missing latitudes (excluding 7.25-8.0 which are mostly ocean)
    missing_lats = [lat for lat in expected_lats if lat not in output_lats and lat > 8.0]
    if missing_lats:
        print(f"\n⚠ WARNING: Missing {len(missing_lats)} latitude bands: {sorted(missing_lats)[:10]}...")
        print("  This may cause white horizontal stripes on the map")

    # Save to file
    script_dir = os.path.dirname(__file__) or '.'
    output_path = os.path.join(script_dir, 'grid_data.json')
    with open(output_path, 'w') as f:
        json.dump(output, f)

    file_size = os.path.getsize(output_path) / (1024 * 1024)
    print(f"\n✓ Saved {valid_count} points to {output_path}")
    print(f"  File size: {file_size:.2f} MB")
    print(f"  Resolution: {GRID_CONFIG['resolution']}°")
    print(f"  Generated at: {output['metadata']['generated_at_ist']}")

    # Final status
    if not missing_lats:
        print("  ✓ Full latitude coverage (no gaps)")
    else:
        print(f"  ⚠ {len(missing_lats)} latitude gaps remain")

    return output


if __name__ == '__main__':
    generate_grid_data()
