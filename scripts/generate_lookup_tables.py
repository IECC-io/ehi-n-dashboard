#!/usr/bin/env python3
"""Regenerate the EHI-N* lookup tables from the corrected model.

The tables shipped with the site were generated 2026-03-23 from the US
reference body (83.6 kg, 1.69 m) and before the Region VI classification fix.
This regenerates them from src/heatindex_ek.py with the Indian reference body
(65 kg, 1.65 m), matching the validation analysis and the technical report.

Grid and file layout are unchanged, so ehi_lookup.py needs no modification:
    Ta  -40 .. 60 C   in 0.5 C steps   (201 columns)
    RH    0 .. 100 %  in 1 % steps     (101 rows)
    -> 20301 entries per table, 32 tables (MET 3-6 x sw0..sw1000 + shade/sun)

Usage:
    python3 scripts/generate_lookup_tables.py --model /path/to/heatindex_ek.py
    python3 scripts/generate_lookup_tables.py --met 6 --sw 800     # one table
    python3 scripts/generate_lookup_tables.py --out lookup_tables_new
"""

import argparse
import importlib.util
import json
import os
import sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Indian reference body, matching compute_ehi_climatology.py and the report.
M_INDIA = 65.0    # kg
H_INDIA = 1.65    # m

# Metabolic rates at the ISO 8996 standard 58.15 W/m^2 per MET, matching the
# occupational intensities quoted in the technical report. Note this differs
# from compute_ehi_climatology.py, which used a flat 60 W/m^2 per MET
# (180/240/300/360); the validation figures were produced with those values.
MET_TO_QM = {3: 174.0, 4: 233.0, 5: 291.0, 6: 349.0}

SW_LEVELS = [0, 200, 400, 600, 800, 1000]

TA_MIN, TA_MAX, TA_STEP = -40.0, 60.0, 0.5
RH_MIN, RH_MAX, RH_STEP = 0, 100, 1

# Zone thresholds are read off the solver's equivalent variable, not the EHI
# value, so the classification matches find_eqvar exactly.
EQVAR_TO_ZONE = {
    'phi': 1,      # Region I
    'Rf': 2,       # Region II/III - reported jointly by the solver
    'Rs': 4,       # Region IV
    'Rs*': 5,      # Region V
    'dTcdt': 6,    # Region VI
}

# find_eqvar returns Regions II and III jointly as 'Rf'. They are separated the
# same way modifiedheatindex() does it (heatindex_ek.py, "if Pa0 > pvstar(T)"):
# Region II is where the reference vapour pressure exceeds saturation at the
# computed heat index, i.e. the reference atmosphere is supersaturated there;
# Region III is the ordinary case. This is a property of the Steadman reference
# scale, not of the subject's thermoregulation.


def load_model(path):
    spec = importlib.util.spec_from_file_location('heatindex_ek', path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules['heatindex_ek'] = mod
    spec.loader.exec_module(mod)
    return mod


def solar_to_qs(sw):
    """Body-absorbed solar flux (W/m^2) from downwelling shortwave (W/m^2).

    0.7 skin absorptivity x 0.26 effective projected-area fraction for a
    standing person: SW 715 -> Qs 130 W/m^2, the value used in the report's
    EHI-6* lookup table.
    """
    return 0.7 * 0.26 * sw


def qs_to_mrt(qs, ta_k, model):
    """Invert Qsolar() to the mean radiant temperature giving this Qs.

    modifiedheatindex() takes MRT, not Qs, and derives Qs internally via
    Qsolar(mrt, Ta, 0.85) = epsilon * phi_rad * sigma * (mrt^4 - Ta^4).
    """
    if qs <= 0:
        return 0.0
    k = model.epsilon * 0.85 * model.sigma
    return (qs / k + ta_k ** 4) ** 0.25


def build_table(model, met, sw, verbose=True):
    qm = MET_TO_QM[met]
    qs = solar_to_qs(sw)
    A = 0.202 * (M_INDIA ** 0.425) * (H_INDIA ** 0.725)
    C = model.cpc * M_INDIA / A

    n_ta = int(round((TA_MAX - TA_MIN) / TA_STEP)) + 1
    data = {}
    failures = 0

    for i in range(n_ta):
        ta_c = TA_MIN + i * TA_STEP
        ta_k = ta_c + 273.15
        mrt = qs_to_mrt(qs, ta_k, model)
        key = f'{ta_c:.1f}'
        row = {}
        for rh in range(RH_MIN, RH_MAX + 1, RH_STEP):
            try:
                ehi_k = model.modifiedheatindex(ta_k, rh / 100.0, qm, mrt,
                                                H_INDIA, M_INDIA)
                name, _ = model.find_eqvar(ta_k, rh / 100.0, qm, qs, A, C)
                zone = EQVAR_TO_ZONE.get(name, 0)
                if zone == 2 and model.Pa0 <= model.pvstar(float(ehi_k)):
                    zone = 3
                row[str(rh)] = [round(float(ehi_k) - 273.15, 2), zone]
            except Exception:
                failures += 1
                row[str(rh)] = [None, 0]
        data[key] = row
        if verbose and i % 40 == 0:
            print(f'    Ta {ta_c:+6.1f} C  ({i + 1}/{n_ta})', flush=True)

    return {
        'metadata': {
            'met_level': met,
            'met_watts': int(qm),
            'sw_wm2': sw,
            'qs_body_wm2': round(qs, 1),
            'temp_min_c': TA_MIN, 'temp_max_c': TA_MAX, 'temp_step_c': TA_STEP,
            'rh_min_pct': RH_MIN, 'rh_max_pct': RH_MAX, 'rh_step_pct': RH_STEP,
            'height_m': H_INDIA, 'mass_kg': M_INDIA,
            'body': 'Indian reference adult',
            'model': 'heatindex_ek.py (Region VI: dTcdt > 0 at Rs_min)',
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'total_entries': n_ta * (RH_MAX - RH_MIN + 1),
            'failed_entries': failures,
        },
        'data': data,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--model', default=os.path.expanduser(
        '~/Desktop/ekilic-coder/EHI-Validation/src/heatindex_ek.py'))
    ap.add_argument('--out', default=os.path.join(ROOT, 'lookup_tables_new'))
    ap.add_argument('--met', type=int, choices=[3, 4, 5, 6],
                    help='generate a single MET level')
    ap.add_argument('--sw', type=int, choices=SW_LEVELS,
                    help='generate a single SW level')
    ap.add_argument('--force', action='store_true',
                    help='regenerate tables that already exist')
    args = ap.parse_args()

    print(f'Model:  {args.model}')
    model = load_model(args.model)
    A = 0.202 * (M_INDIA ** 0.425) * (H_INDIA ** 0.725)
    print(f'Body:   {M_INDIA} kg, {H_INDIA} m, A = {A:.4f} m^2')
    print(f'Output: {args.out}\n')
    os.makedirs(args.out, exist_ok=True)

    mets = [args.met] if args.met else [3, 4, 5, 6]
    sws = [args.sw] if args.sw is not None else SW_LEVELS

    for met in mets:
        for sw in sws:
            path = os.path.join(args.out, f'ehi_met{met}_sw{sw}.json')
            if os.path.exists(path) and not args.force:
                print(f'  MET {met}, SW {sw} W/m^2 - already present, skipping')
                continue
            print(f'  MET {met}, SW {sw} W/m^2 ...', flush=True)
            table = build_table(model, met, sw)
            with open(path, 'w') as fh:
                json.dump(table, fh, separators=(',', ':'))
            md = table['metadata']
            print(f'    -> {os.path.basename(path)}  '
                  f'({md["total_entries"]} entries, {md["failed_entries"]} failed)\n')

            # shade/sun aliases preserve the legacy filenames ehi_lookup.py
            # falls back to: shade == SW 0, sun == SW 800.
            alias = {0: 'shade', 800: 'sun'}.get(sw)
            if alias and not args.sw:
                ap_ = os.path.join(args.out, f'ehi_met{met}_{alias}.json')
                with open(ap_, 'w') as fh:
                    json.dump(table, fh, separators=(',', ':'))
                print(f'    -> {os.path.basename(ap_)} (alias)\n')

    print('Done. Review, then replace lookup_tables/ with this directory.')


if __name__ == '__main__':
    main()
