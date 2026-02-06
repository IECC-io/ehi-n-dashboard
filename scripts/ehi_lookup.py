"""
EHI Lookup Table Module

Provides fast EHI and zone lookups using pre-computed tables.
No scipy or NumbaMinpack dependencies - just JSON files.

Usage:
    from ehi_lookup import EHILookup

    lookup = EHILookup()
    ehi, zone = lookup.get_ehi_zone(temp_c=35.0, rh_percent=80, met_level=4, sun='shade')
"""

import json
import os

class EHILookup:
    """Fast EHI lookup using pre-computed tables."""

    def __init__(self, tables_dir=None):
        """
        Initialize the EHI lookup with pre-computed tables.

        Args:
            tables_dir: Directory containing the lookup table JSON files.
                       If None, looks in ./lookup_tables/ or ../lookup_tables/
        """
        if tables_dir is None:
            # Try to find tables directory
            possible_dirs = [
                os.path.join(os.path.dirname(__file__), 'lookup_tables'),
                os.path.join(os.path.dirname(__file__), '..', 'lookup_tables'),
                'lookup_tables',
                '../lookup_tables',
            ]
            for d in possible_dirs:
                if os.path.exists(d):
                    tables_dir = d
                    break
            else:
                raise FileNotFoundError("Could not find lookup_tables directory")

        self.tables_dir = tables_dir
        self.tables = {}
        self._load_tables()

    def _load_tables(self):
        """Load all lookup tables into memory."""
        met_levels = [3, 4, 5, 6]
        sun_conditions = ['shade', 'sun']

        for met in met_levels:
            for sun in sun_conditions:
                key = f"met{met}_{sun}"
                filepath = os.path.join(self.tables_dir, f"ehi_{key}.json")

                if os.path.exists(filepath):
                    with open(filepath, 'r') as f:
                        self.tables[key] = json.load(f)
                else:
                    print(f"Warning: Table not found: {filepath}")

        print(f"Loaded {len(self.tables)} EHI lookup tables")

    def get_ehi_zone(self, temp_c, rh_percent, met_level, sun):
        """
        Get EHI and zone from lookup tables.

        Args:
            temp_c: Temperature in Celsius
            rh_percent: Relative humidity in percent (0-100)
            met_level: MET level (3, 4, 5, or 6)
            sun: Sun condition ('shade' or 'sun')

        Returns:
            (ehi, zone) tuple where ehi is in Celsius and zone is 1-6
        """
        key = f"met{met_level}_{sun}"

        if key not in self.tables:
            raise ValueError(f"No table loaded for {key}")

        table = self.tables[key]
        metadata = table['metadata']
        data = table['data']

        # Clamp temperature and humidity to table bounds
        temp_c = max(metadata['temp_min_c'], min(metadata['temp_max_c'], temp_c))
        rh_percent = max(metadata['rh_min_pct'], min(metadata['rh_max_pct'], rh_percent))

        # Round to nearest step
        temp_step = metadata['temp_step_c']
        rh_step = metadata['rh_step_pct']

        temp_rounded = round(temp_c / temp_step) * temp_step
        rh_rounded = int(round(rh_percent / rh_step) * rh_step)

        # Create keys
        temp_key = f"{temp_rounded:.1f}"
        rh_key = str(rh_rounded)

        # Lookup
        if temp_key in data and rh_key in data[temp_key]:
            result = data[temp_key][rh_key]
            return result[0], result[1]  # [ehi, zone]
        else:
            # Fallback: find nearest
            return self._find_nearest(data, temp_c, rh_percent)

    def _find_nearest(self, data, temp_c, rh_percent):
        """Find nearest entry if exact match not found."""
        temp_key = f"{round(temp_c * 2) / 2:.1f}"  # Round to 0.5
        rh_key = str(round(rh_percent))

        if temp_key in data:
            if rh_key in data[temp_key]:
                result = data[temp_key][rh_key]
                return result[0], result[1]
            # Try nearest humidity
            for offset in range(1, 10):
                for rh_try in [rh_key - offset, rh_key + offset]:
                    if str(rh_try) in data[temp_key]:
                        result = data[temp_key][str(rh_try)]
                        return result[0], result[1]

        return None, 0


# Global instance for convenience
_lookup_instance = None

def get_lookup():
    """Get or create the global EHI lookup instance."""
    global _lookup_instance
    if _lookup_instance is None:
        _lookup_instance = EHILookup()
    return _lookup_instance

def lookup_ehi_zone(temp_c, rh_percent, met_level, sun):
    """
    Convenience function to look up EHI and zone.

    Args:
        temp_c: Temperature in Celsius
        rh_percent: Relative humidity in percent (0-100)
        met_level: MET level (3, 4, 5, or 6)
        sun: Sun condition ('shade' or 'sun')

    Returns:
        (ehi, zone) tuple
    """
    return get_lookup().get_ehi_zone(temp_c, rh_percent, met_level, sun)


# Also export constants used by other modules
cpc = 3492.0  # J/kg/K, specific heat capacity of body


if __name__ == '__main__':
    # Test the lookup
    lookup = EHILookup()

    test_cases = [
        (30, 50, 3, 'shade'),
        (35, 80, 4, 'sun'),
        (40, 90, 5, 'shade'),
        (45, 70, 6, 'sun'),
    ]

    print("\nTest lookups:")
    for temp, rh, met, sun in test_cases:
        ehi, zone = lookup.get_ehi_zone(temp, rh, met, sun)
        print(f"  T={temp}°C, RH={rh}%, MET={met}, {sun}: EHI={ehi}°C, Zone={zone}")
