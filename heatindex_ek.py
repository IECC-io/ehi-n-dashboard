# Version 1.0 released by Yi-Chuan Lu on May 18, 2022.
# Modified by Alex Goodman on November 22, 2022 to support vectorization.
# Modified by Elif Kilic on August 6, 2024 to convert dewpoint temperature to relative humidity
# Modified by Elif Kilic to January 15, 2025 to include additional metabolic heat gain term
# Modified by Elif Kilic to February 6, 2025 to include mean radiant temperature and absorbed radiation heat gain terms
# Modified to include BOTH sweat limit (2 L/hr) AND vasodilation limit (7.8 L/min)
#
# When using this code, please cite:
#
# @article{20heatindex,
#   Title   = {Extending the Heat Index},
#   Author  = {Yi-Chuan Lu and David M. Romps},
#   Journal = {Journal of Applied Meteorology and Climatology},
#   Year    = {2022},
#   Volume  = {in press}
# }
#
# This heatindex function returns the Heat Index in Kelvin. The inputs are:
# - T, the temperature in Kelvin
# - RH, the relative humidity, which is a value from 0 to 1
# - Qm, the metabolic heat flux, which is a value in W/m^2
# - mrt, the mean radiant temperature, which is a value in Kelvin

import math
import numpy as np
from NumbaMinpack import hybrd, minpack_sig
from numba import njit, cfunc, vectorize, guvectorize, float64

# Thermodynamic parameters
Ttrip = 273.16       # K
ptrip = 611.65       # Pa
E0v   = 2.3740e6     # J/kg
E0s   = 0.3337e6     # J/kg
rgasa = 287.04       # J/kg/K
rgasv = 461.         # J/kg/K
cva   = 719.         # J/kg/K
cvv   = 1418.        # J/kg/K
cvl   = 4119.        # J/kg/K
cvs   = 1861         # J/kg/K
cpa   = cva + rgasa
cpv   = cvv + rgasv

# The relative humidity (taking Kelvin inputs from weather file)
@vectorize([float64(float64, float64)], nopython=True)
def relativehumidity(Ta, dT):
    e = 6.11 * np.exp((17.625 * (dT - 273.15)) / (243.04 + (dT - 273.15)))
    es = 6.11 * np.exp((17.625 * (Ta - 273.15)) / (243.04 + (Ta - 273.15)))
    RH = (e / es)
    return RH

# The saturation vapor pressure
@njit
def pvstar(T):
    if T == 0:
        return 0.0
    elif T<Ttrip:
        return ptrip * (T/Ttrip)**((cpv-cvs)/rgasv) * math.exp((E0v + E0s -(cvv-cvs)*Ttrip)/rgasv * (1/Ttrip - 1/T) )
    else:
        return ptrip * (T/Ttrip)**((cpv-cvl)/rgasv) * math.exp((E0v       -(cvv-cvl)*Ttrip)/rgasv * (1/Ttrip - 1/T) )

# The latent heat of vaporization of water
@njit
def Le(T):
    return (E0v + (cvv-cvl)*(T-Ttrip) + rgasv*T)

# Thermoregulatory parameters
sigma       = 5.67e-8                     # W/m^2/K^4 , Stefan-Boltzmann constant
epsilon     = 0.97                        #           , emissivity of surface, steadman1979
M           = 83.6                        # kg        , mass of average US adults, fryar2018
H           = 1.69                        # m         , height of average US adults, fryar2018
A           = 0.202*(M**0.425)*(H**0.725) # m^2       , DuBois formula, parson2014
cpc         = 3492.                       # J/kg/K    , specific heat capacity of core, gagge1972
C           = M*cpc/A                     #           , heat capacity of core
r           = 124.                        # Pa/K      , Zf/Rf, steadman1979
phi_salt    = 0.9                         #           , vapor saturation pressure level of saline solution, steadman1979
Tc          = 310.                        # K         , core temperature, steadman1979
Pc          = phi_salt * pvstar(Tc)       #           , core vapor pressure
L           = Le(310.)                    #           , latent heat of vaporization at 310 K
p           = 1.013e5                     # Pa        , atmospheric pressure
eta         = 1.43e-6                     # kg/J      , "inhaled mass" / "metabolic rate", steadman1979
Pa0         = 1.6e3                       # Pa        , reference air vapor pressure in regions III, IV, V, VI, steadman1979

# Thermoregulatory functions
@njit
def _compute_A_C(M, H, cpc=3492.0):
    A = 0.202 * (M**0.425) * (H**0.725)
    C = M * cpc / A
    return A, C

@njit
def Qv(Ta, Pa, Qm):
    return eta * Qm * (cpa * (Tc - Ta) + L * rgasa / (p * rgasv) * (Pc - Pa))

@njit
def Qsolar(mrt, Ta, phi_rad=0.85):
    if mrt == 0:
        return 0.0
    elif mrt == 0.5:
        mrt = Ta + 5.0
    elif mrt == 1:
        mrt = Ta + 15.0
    return epsilon * phi_rad * sigma * (mrt**4 - Ta**4)

@njit
def Zs(Rs): # mass transfer resistance through skin, Pa m^2/W
    return (52.1 if Rs == 0.0387 else 6.0e8 * Rs**5)

@njit
def Ra(Ts,Ta): # heat transfer resistance through air, exposed part of skin, K m^2/W
    hc      = 17.4
    phi_rad = 0.85
    hr      = epsilon * phi_rad * sigma* (Ts**2 + Ta**2)*(Ts + Ta)
    return 1/(hc+hr)

@njit
def Ra_bar(Tf,Ta): # heat transfer resistance through air, clothed part of skin, K m^2/W
    hc      = 11.6
    phi_rad = 0.79
    hr      = epsilon * phi_rad * sigma* (Tf**2 + Ta**2)*(Tf + Ta)
    return 1/(hc+hr)

@njit
def Ra_un(Ts,Ta): # heat transfer resistance through air, when being naked, K m^2/W
    hc      = 12.3
    phi_rad = 0.80
    hr      = epsilon * phi_rad * sigma* (Ts**2 + Ta**2)*(Ts + Ta)
    return 1/(hc+hr)

@njit
def V_dot(limit, A): # heat transfer resistance through skin with enforced vasodilation limit, m^2K/W 
    """ input limit is in L/min """
    kmin = 5.28                # W/K/m^2     , conductance of tissue
    rho  = 1.0e3               # kg/m^3      , density of blood
    c    = 4184.               # J/kg/K      , specific heat of blood
    v = limit / 1000 / 60      # m^3/s       , convert limit from L/min to m^3/s
    Rs = 1 / (((v * rho * c)/A) + kmin)
    return Rs

@njit
def E_cap(Ts: float, Smax_Lph: float, A: float) -> float:
    """Maximum sweat production (W m⁻²) = (Smax/3600) * Le(Ts) / area."""
    return (Smax_Lph / 3600.0) * Le(Ts) / A

@njit
def E_req(Ts: float, Pa: float, Rs: float) -> float:
    """
    Required evaporation: minimum of air-limited and surface-limited.
    This is what the environment/physics demands.
    """
    # Air-limited evaporation
    E_air = (Pc - Pa) / (Zs(Rs) + Za_un)
    # Surface-limited evaporation
    E_surface = (phi_salt * pvstar(Ts) - Pa) / Za_un
    # Physical requirement is the minimum
    return min(E_air, E_surface)

@njit
def apply_sweat_efficiency_iso7933(E_required, E_capacity):
    """
    Apply ISO 7933 sweating efficiency model.
    
    η = 1 - 0.5*w²  where w = E_required/E_capacity
    E_actual = min(E_required * η, E_capacity)
    
    Args:
        E_required: Required evaporation rate (W/m²)
        E_capacity: Maximum sweat capacity (W/m²)
    
    Returns:
        E_actual: Actual evaporation rate (W/m²)
    """
    
    # Skin wettedness
    w = E_required / E_capacity
    
    # ISO 7933 efficiency formula
    eta = 1.0 - 0.5 * w * w
    if eta < 0.0:
        eta = 0.0
    
    # Apply efficiency
    E_actual = E_required * eta
    
    # Cap at sweat capacity
    if E_actual > E_capacity:
        E_actual = E_capacity
    
    return E_actual

@njit
def smooth_min(E_req, E_cap, k=0.2):
    """
    Smooth minimum using log-sum-exp.
    Approximates min(E_req, E_cap) with continuous derivative.
    """
    excess = E_req - E_cap
    if excess < -10.0/k:
        return E_req
    elif excess > 10.0/k:
        return E_cap
    else:
        return E_req - (1.0/k) * np.log(1.0 + np.exp(k * excess))
 
Za     = 60.6/17.4  # Pa m^2/W, mass transfer resistance through air, exposed part of skin
Za_bar = 60.6/11.6  # Pa m^2/W, mass transfer resistance through air, clothed part of skin
Za_un  = 60.6/12.3  # Pa m^2/W, mass transfer resistance through air, when being naked

# tolerance and maximum iteration for the root solver
tol     = 1e-8
tolT    = 1e-8
maxIter = 100

@njit
def find_Ts1(Ts, Ta, Pa, Rs):
    return (Ts-Ta)/Ra(Ts,Ta) + (Pc-Pa)/(Zs(Rs)+Za) - (Tc-Ts)/Rs

@njit
def find_Tf1(Tf, Ta, Pa, Rs):
    return (Tf-Ta)/Ra_bar(Tf,Ta) + (Pc-Pa)/(Zs(Rs)+Za_bar) - (Tc-Tf)/Rs

@njit
def find_Ts2(Ts, Ta, Pa, Qm, Qs):
    return (Ts-Ta)/Ra_un(Ts,Ta)+(Pc-Pa)/(Zs((Tc-Ts)/((Qm + Qs)-Qv(Ta,Pa,Qm)))+Za_un)-((Qm + Qs)-Qv(Ta,Pa,Qm))

@njit
def find_Tf2(Tf, Ta, Pa, Rs, Ts_bar):
    return (Tf-Ta)/Ra_bar(Tf,Ta) + (Pc-Pa)*(Tf-Ta)/((Zs(Rs)+Za_bar)*(Tf-Ta)+r*Ra_bar(Tf,Ta)*(Ts_bar-Tf)) - (Tc-Ts_bar)/Rs

@njit
def find_Ts3(Ts, Ta, Pa, Qm, Qs):
    return (Ts-Ta)/Ra_un(Ts,Ta) + (phi_salt*pvstar(Ts)-Pa)/Za_un -((Qm + Qs)-Qv(Ta,Pa,Qm))

@njit
def find_Ts4(Ts, Ta, Pa, Rs):
    return (Tc-Ts)/Rs - (Ts-Ta)/Ra_un(Ts, Ta) - (phi_salt*pvstar(Ts) - Pa)/Za_un

@njit
def find_Ts5(Ts, Ta, Pa, Rs, E_actual):
    """
    Find skin temperature when evaporation is limited.
    Heat balance: (Tc - Ts)/Rs = (Ts - Ta)/Ra + E_actual
    """
    return (Tc - Ts)/Rs - (Ts - Ta)/Ra_un(Ts, Ta) - E_actual


# Given air temperature and relative humidity, returns the equivalent variables
@njit
def find_eqvar(Ta, RH, Qm, Qs, A, C):
    Rs_min = V_dot(7.8, A)      # VASODILATION LIMIT: 7.8 L/min blood flow
    Smax_Lph = 20.0           # SWEAT LIMIT: 2 L/hr sweat rate
    Qt = Qm + Qs
    Pa = RH * pvstar(Ta)
    Rs = 0.0387
    phi = 0.84
    dTcdt = 0.

    m = (Pc - Pa) / (Zs(Rs) + Za)
    m_bar = (Pc - Pa) / (Zs(Rs) + Za_bar)
    Ts_bar = None

    # Solve for initial Ts and Rs 
    Ts = solve(find_Ts1, max(0, min(Tc, Ta) - Rs * abs(m)), max(Tc, Ta) + Rs * abs(m), tol, maxIter, Ta, Pa, Rs)
    Tf = solve(find_Tf1, max(0, min(Tc, Ta) - Rs * abs(m_bar)), max(Tc, Ta) + Rs * abs(m_bar), tol, maxIter, Ta, Pa, Rs)

    # Heat flux equation for region 1
    flux1 = Qt - Qv(Ta, Pa, Qm) - (1 - phi) * (Tc - Ts) / Rs
    
    # Heat flux equation for regions 2 and 3
    flux2 = Qt - Qv(Ta, Pa, Qm) - (1 - phi) * (Tc - Ts) / Rs - phi * (Tc - Tf) / Rs

    # Heat flux equation for regions 4, 5, and 6
    # flux3 = Qt - eta * Qt * (cpa * (Tc - Ta) + ((L * rgasa) / (rgasv * p)) * (Pc - Pa)) - 0.80 * epsilon * sigma * (Tc ** 4 - Ta ** 4) - 12.3 * (Tc - Ta) - ((Pc - Pa) / Za_un)
    flux3 = Qm + Qs - eta * Qm * (cpa * (Tc - Ta) + ((L * rgasa) / (rgasv * p)) * (Pc - Pa)) - 0.80 * epsilon * sigma * (Tc ** 4 - Ta ** 4) - 12.3 * (Tc - Ta) - ((Pc - Pa) / Za_un)

    if flux1 <= 0: # Entering region 1
        eqvar_name = "phi"
        phi = 1 - (Qt - Qv(Ta, Pa, Qm)) * Rs / (Tc - Ts)
        Rf = math.inf
        return eqvar_name, [phi, Rf, Rs, dTcdt, Ts, Tf]

    elif flux2 <= 0: # Entering region 2 and 3
        eqvar_name = "Rf"
        Ts_bar = Tc - (Qt - Qv(Ta, Pa, Qm)) * Rs / phi + (1 / phi - 1) * (Tc - Ts)
        Tf = solve(find_Tf2, Ta, Ts_bar, tol, maxIter, Ta, Pa, Rs, Ts_bar)
        Rf = Ra_bar(Tf, Ta) * (Ts_bar - Tf) / (Tf - Ta)
        return eqvar_name, [phi, Rf, Rs, dTcdt, Ts, Tf]
    
    else: # region IV,V,VI
        Rf = 0
        if (flux3 < 0) : # region IV,V potentially
            # Ts = solve(find_Ts2,0,Tc,tol,maxIter,Ta,Pa, Qm, Qs)
            Ts = solve(find_Ts2,0,Tc,tol,maxIter,Ta,Pa, Qm, Qs)
            Rs = (Tc-Ts)/(Qt-Qv(Ta,Pa,Qm))
            
            # CHECK BOTH LIMITS
            # 1. Check vasodilation limit
            vaso_limited = (Rs < Rs_min)
            
            # 2. Check sweat limit
            E_required = E_req(Ts, Pa, Rs)
            E_capacity = E_cap(Ts, Smax_Lph, A)
            sweat_limited = (E_required > E_capacity)
            
            if vaso_limited or sweat_limited:
                # At least one limit is hit -> Region VI (dTcdt)
                Rs = max(Rs, Rs_min)
                Ts = solve(find_Ts4, 0, 400, tol, maxIter, Ta, Pa, Rs)
                
                # Calculate actual evaporation (limited by sweat capacity)
                E_required = E_req(Ts, Pa, Rs)
                E_actual = smooth_min(E_required, E_capacity)

                Ts = solve(find_Ts5, 0, 400, tol, maxIter, Ta, Pa, Rs, E_actual)

                # Heat balance at core with limited evaporation
                fluxC = Qt - Qv(Ta, Pa, Qm) - (Tc - Ts)/Rs
                dTcdt = fluxC / C
                eqvar_name = "dTcdt"
                return eqvar_name, [phi, Rf, Rs, dTcdt, Ts, Tf]
            else:
                # Neither limit hit -> Region IV or V
                Ps = Pc - (Pc - Pa) * Zs(Rs) / (Zs(Rs) + Za_un)
                
                if (Ps <= phi_salt * pvstar(Ts)) :  # region V
                    eqvar_name = 'Rs'
                    return eqvar_name, [phi, Rf, Rs, dTcdt, Ts, Tf]
                else:  # Region IV (surface-limited)
                    Ts = solve(find_Ts3, 0, Tc, tol, maxIter, Ta, Pa, Qm, Qs)
                    Rs = (Tc - Ts) / (Qt - Qv(Ta, Pa, Qm))
                    eqvar_name = "Rs*"
                    return eqvar_name, [phi, Rf, Rs, dTcdt, Ts, Tf]
        else: # region VI from start
            Rs = Rs_min
            Ts = solve(find_Ts4, 0, 400, tol, maxIter, Ta, Pa, Rs)
            
            # Calculate actual evaporation (limited by sweat capacity)
            E_required = E_req(Ts, Pa, Rs)
            E_capacity = E_cap(Ts, Smax_Lph, A)
            E_actual = smooth_min(E_required, E_capacity)

            Ts = solve(find_Ts5, 0, 400, tol, maxIter, Ta, Pa, Rs, E_actual)
            
            fluxC = Qt - Qv(Ta, Pa, Qm) - (Tc - Ts)/Rs
            dTcdt = fluxC / C
            eqvar_name = "dTcdt"
            return eqvar_name, [phi, Rf, Rs, dTcdt, Ts, Tf]

@cfunc(minpack_sig)
def f1(T, fvec, args):
    eqvar = args[0]
    Qs = args[1]

    Ts = T[0]
    Tf = T[1]
    Ta = T[2]
    Rs = 0.0387
    Pa = pvstar(Ta)
    fvec[0] = find_Ts1(Ts, Ta, Pa, Rs)
    fvec[1] = find_Tf1(Tf, Ta, Pa, Rs)
    fvec[2] = 1-(180.+Qs-Qv(Ta,Pa,180.))*Rs/(Tc-Ts) - eqvar

@cfunc(minpack_sig)
def f23(T, fvec, args):
    eqvar = args[0]
    Qs = args[1]

    Ts = T[0]
    Tf = T[1]
    Ta = T[2]
    Pa = min(Pa0, pvstar(Ta))
    Rs = 0.0387
    phi = 0.84
    Ts_bar = Tc - (180.+Qs-Qv(Ta,Pa,180.))*Rs/phi + (1/phi -1)*(Tc-Ts)
    fvec[0] = find_Ts1(Ts, Ta, Pa, Rs)
    fvec[1] = find_Tf2(Tf, Ta, Pa, Rs, Ts_bar)
    fvec[2] = Ra_bar(Tf,Ta)*(Ts_bar-Tf)/(Tf-Ta) - eqvar

@cfunc(minpack_sig)
def f45(T, fvec, args):
    eqvar = args[0]
    Qs = args[1]

    Ts = T[0]
    Ta = T[1]
    Pa = Pa0

    # fvec[0] = find_Ts2(Ts, Ta, Pa, 180, 0)
    # fvec[1] = (Tc-Ts)/(180 - Qv(Ta, Pa, 180)) - eqvar

    Ps = Pc - (Pc-Pa)* Zs(eqvar)/(Zs(eqvar)+Za_un)

    if (Ps > phi_salt * pvstar(Ts)):
        fvec[0] = find_Ts3(Ts, Ta, Pa, 180, Qs)
    else:
        fvec[0] = find_Ts2(Ts, Ta, Pa, 180, Qs)
    fvec[1] = (Tc-Ts)/(180 + Qs - Qv(Ta, Pa, 180)) - eqvar

# @cfunc(minpack_sig)
# def f4(T, fvec, args, Qs):
#     """Inversion for Region IV (internal-limited) - always uses find_Ts2"""
#     eqvar = args[0]
#     Ts = T[0]
#     Ta = T[1]
#     Pa = Pa0
    
#     fvec[0] = find_Ts2(Ts, Ta, Pa, 180, 0)
#     fvec[1] = (Tc-Ts)/(180 - Qv(Ta, Pa, 180, Qs)) - eqvar

# @cfunc(minpack_sig)
# def f5(T, fvec, args, Qs):
#     """Inversion for Region V (surface-saturated) - always uses find_Ts3"""
#     eqvar = args[0]
#     Ts = T[0]
#     Ta = T[1]
#     Pa = Pa0
    
#     fvec[0] = find_Ts3(Ts, Ta, Pa, 180, 0)
    # fvec[1] = (Tc-Ts)/(180 - Qv(Ta, Pa, 180, Qs)) - eqvar

@njit
def f6(T, eqvar, A, C, Qs):
    Ta = T
    Pa = Pa0
    Ts = solve(find_Ts4,0,800,tol,maxIter,Ta, Pa, V_dot(7.8, A))
    fluxC = 180 + Qs - Qv(Ta, Pa, 180) - (Tc - Ts)/V_dot(7.8, A)

    return fluxC/C - eqvar

f1_ptr = f1.address
f23_ptr = f23.address
f45_ptr = f45.address
# f4_ptr = f4.address
# f5_ptr = f5.address

@njit
def solve(f, x1, x2, tol, maxIter, *args):
    a, b = x1, x2
    fa = f(a, *args)
    fb = f(b, *args)

    # Expanding upper bound
    for _ in range(30):
        b += 1.0
        fb = f(b, *args)
        if fa * fb <= 0:
            break

    if fa * fb > 0:
        # No sign change error
        print("solve: unable to bracket root, returning NaN", a, b, fa, fb)
        return math.nan

    # Continue with bracketed root
    for _ in range(maxIter):
        c = 0.5 * (a + b)
        fc = f(c, *args)
        if fa * fc <= 0:
            b, fb = c, fc
        else:
            a, fa = c, fc
        if abs(b - a) < tol:
            return 0.5 * (a + b)

    print("solve: maxIter reached", a, b, fa, fb)
    return math.nan

@njit
def hybrd_multi_branch(f_ptr, x0_base, args, target_Rs):
    """
    Run hybrd with multiple initial guesses and return the solution
    with highest Ta (heat index).
    """
    # Define multiple initial guesses
    x0_variations = [
        x0_base,
        np.array([x0_base[0] - 5, x0_base[1]]),
        np.array([x0_base[0] + 5, x0_base[1]]),
        np.array([x0_base[0], x0_base[1] - 10]),
        np.array([x0_base[0], x0_base[1] + 10]),
        np.array([Tc - 10, x0_base[1]]),
        np.array([Tc - 2, x0_base[1] + 15]),
    ]
    
    best_Ta = -np.inf
    best_solution = None
    
    for x0 in x0_variations:
        x, fvec, info, _ = hybrd(f_ptr, x0, args)
        
        # Check if converged AND residual is small
        residual = np.linalg.norm(fvec)
        if info == 1 and residual < 1e-6:
            Ta_found = x[1]
            
            # Take highest Ta as "best"
            if Ta_found > best_Ta:
                best_Ta = Ta_found
                best_solution = x
    
    # If no solution found, fall back to original hybrd with x0_base
    if best_solution is None:
        x, fvec, info, _ = hybrd(f_ptr, x0_base, args)
        return x

    return best_solution

@vectorize([float64(float64, float64, float64, float64, float64, float64)], nopython=True)
def modifiedheatindex(Ta, RH, Qm, mrt, H, M):
    A = 0.202 * (M**0.425) * (H**0.725)
    Qs = Qsolar(mrt, Ta, 0.85)
    C = M * cpc / A  # Heat capacity of core, J/K
    dic = {"phi":0,"Rf":1,"Rs":2,"Rs*":2,"dTcdt":3}
    eqvar_name, eqvars = find_eqvar(Ta, RH, Qm, Qs, A, C)
    Ts, Tf = eqvars[-2], eqvars[-1]
    eqvar = eqvars[dic[eqvar_name]]

    args = np.zeros(2)
    args[0] = eqvar
    args[1] = Qs
    
    x0 = np.zeros(3)
    x0[0] = Ts
    x0[1] = Tf
    x0[2] = Ta

    if eqvar_name == "phi":
        f = f1_ptr
        region = 'I'
    elif eqvar_name == "Rf":
        f = f23_ptr
        region = 'IIorIII'
    elif eqvar_name.startswith("Rs"):
        x0 = np.array([x0[0], x0[2]])
        f = f45_ptr
        region = 'IVorV'
    else:
        region = 'VI'
    # elif eqvar_name == "Rs":
    #     x0 = np.array([x0[0], x0[2]])
    #     f = f4_ptr
    #     region = 'IV'
    # elif eqvar_name == "Rs*":
    #     x0 = np.array([x0[0], x0[2]])
    #     f = f5_ptr
    #     region = 'V'
    # else:
    #     region = 'VI'

    if region == 'VI':
        T = solve(f6, 100, 800, tolT, maxIter, eqvar, A, C, Qs)
    else:
        # x, _, _, _ = hybrd(f, x0, args)
        x = hybrd_multi_branch(f, x0, args, eqvar)
        T = x[-1]
        if region == 'IIorIII':
            if Pa0 > pvstar(T):
                region = 'II'
            else:
                region = 'III'
        else:
            pass

    if Ta == 0.:
        T = 0.

    return T

@guvectorize([(float64[:], float64[:])], '(n)->()', nopython=True)
def modifiedheatindex_gu(inputs, out):
    Ta, RH, Qm, mrt, H, M = inputs
    out[0] = modifiedheatindex(Ta, RH, Qm, mrt, H, M)