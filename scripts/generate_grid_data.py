#!/usr/bin/env python3
"""
Generate pre-computed EHI grid data for all of India.
Fetches weather from WeatherAPI.com and computes EHI/zones using lookup tables.
Run this hourly via cron to keep data fresh.
"""

import json
import requests
from datetime import datetime
import pytz
import os
import time
import glob as glob_module

# Use lookup tables for EHI calculations (no NumbaMinpack/scipy needed)
from ehi_lookup import EHILookup

print("Using EHI lookup tables")

# Initialize lookup
lookup = EHILookup()

# WeatherAPI.com API key
WEATHER_API_KEY = '4753e967970b4abca6b63520261401'

# Grid configuration - 0.5° resolution (WeatherAPI has generous limits: 1M calls/month)
GRID_CONFIG = {
    'lat_min': 7.0,
    'lat_max': 37.0,
    'lon_min': 68.0,
    'lon_max': 97.0,
    'resolution': 0.5
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


def fetch_weather_single(lat, lon):
    """Fetch weather data for a single point from WeatherAPI.com."""
    url = f'http://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={lat},{lon}'

    try:
        response = requests.get(url, timeout=30)
        data = response.json()

        if 'current' in data:
            # Extract location name from response
            location = data.get('location', {})
            city = location.get('name', '')
            region = location.get('region', '')

            # Build location string
            if city and region and city != region:
                location_name = f"{city}, {region}"
            elif city:
                location_name = city
            elif region:
                location_name = region
            else:
                location_name = f"{lat}, {lon}"

            return {
                'temp': data['current']['temp_c'],
                'rh': data['current']['humidity'],
                'location': location_name
            }
        elif 'error' in data:
            print(f"API error for ({lat}, {lon}): {data['error'].get('message', 'Unknown error')}")
            return None
        else:
            return None
    except Exception as e:
        print(f"Error fetching ({lat}, {lon}): {e}")
        return None


def fetch_weather_batch(points, batch_size=1):
    """Fetch weather data from WeatherAPI.com (one point at a time)."""
    all_weather = []

    for i, point in enumerate(points):
        weather = fetch_weather_single(point['lat'], point['lon'])
        all_weather.append(weather)

        # Small delay to be nice to the API (not strictly needed with 1M/month limit)
        if (i + 1) % 10 == 0:
            time.sleep(0.1)

        # Progress every 50 points
        if (i + 1) % 50 == 0 or (i + 1) == len(points):
            print(f"Fetched {i + 1}/{len(points)} points")

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


def generate_grid_data():
    """Main function to generate pre-computed grid data."""
    print("Loading India boundary...")
    india_boundary = load_india_boundary()

    print("Loading district boundaries...")
    districts = load_district_geojsons()

    print("Generating grid points...")
    points = generate_grid_points(india_boundary)
    print(f"Found {len(points)} points within India")

    # Pre-compute district for each point
    print("Mapping points to districts...")
    point_districts = {}
    for i, point in enumerate(points):
        district_name, state_name = find_district_for_point(point['lat'], point['lon'], districts)
        point_districts[(point['lat'], point['lon'])] = (district_name, state_name)
        if (i + 1) % 100 == 0:
            print(f"  Mapped {i + 1}/{len(points)} points to districts")

    print("Fetching weather data...")
    weather_data = fetch_weather_batch(points)

    # Prepare output data structure
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)

    output = {
        'metadata': {
            'generated_at': now.isoformat(),
            'generated_at_ist': now.strftime('%d %b %Y, %I:%M %p IST'),
            'point_count': len(points),
            'resolution_deg': GRID_CONFIG['resolution'],
            'met_levels': [3, 4, 5, 6],
            'sun_conditions': ['shade', 'sun']
        },
        'points': []
    }

    print("Computing EHI and zones for all conditions...")
    for i, (point, weather) in enumerate(zip(points, weather_data)):
        if weather is None:
            continue

        # Get district and state for this point
        district_name, state_name = point_districts.get((point['lat'], point['lon']), (None, None))

        point_data = {
            'lat': point['lat'],
            'lon': point['lon'],
            'location': weather.get('location', f"{point['lat']}, {point['lon']}"),
            'district': district_name,  # Official district name from GeoJSON
            'state': state_name,        # State name
            'temp': weather['temp'],
            'rh': weather['rh'],
            'data': {}
        }

        # Compute for all MET levels and sun conditions
        for met in [3, 4, 5, 6]:
            point_data['data'][f'met{met}'] = {}
            for sun in ['shade', 'sun']:
                ehi, zone = compute_ehi_and_zone(weather['temp'], weather['rh'], met, sun)
                point_data['data'][f'met{met}'][sun] = {
                    'ehi': round(ehi, 1) if ehi is not None else None,
                    'zone': zone
                }

        output['points'].append(point_data)

        if (i + 1) % 100 == 0:
            print(f"Processed {i + 1}/{len(points)} points")

    # Save to file (use relative path for GitHub Actions compatibility)
    output_path = 'grid_data.json'
    with open(output_path, 'w') as f:
        json.dump(output, f)

    print(f"\nSaved {len(output['points'])} points to grid_data.json")
    print(f"Generated at: {output['metadata']['generated_at_ist']}")

    return output


if __name__ == '__main__':
    generate_grid_data()
