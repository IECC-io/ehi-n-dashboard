import math  # Importing math module for mathematical operations like exponentiation.

# Thermodynamic parameters (constants used for calculations related to temperature, pressure, etc.)
Ttrip = 273.16       # Trip temperature in Kelvin (T = 0°C)
ptrip = 611.65       # Saturation vapor pressure at Ttrip in Pascals
E0v   = 2.3740e6     # Latent heat of vaporization for water in J/kg
E0s   = 0.3337e6     # Latent heat of fusion for water in J/kg
rgasa = 287.04       # Specific gas constant for air in J/kg/K
rgasv = 461.        # Specific gas constant for water vapor in J/kg/K
cva   = 719.        # Specific heat capacity of air at constant volume in J/kg/K
cvv   = 1418.       # Specific heat capacity of water vapor at constant volume in J/kg/K
cvl   = 4119.       # Specific heat capacity of liquid water at constant volume in J/kg/K
cvs   = 1861.       # Specific heat capacity of saturated water vapor in J/kg/K
cpa   = cva + rgasa  # Specific heat capacity of air at constant pressure
cpv   = cvv + rgasv  # Specific heat capacity of water vapor at constant pressure

# The saturation vapor pressure function
def pvstar(T):
   if T == 0.0:
       return 0.0  # Return 0 if temperature is 0 (to avoid math errors)
   elif T < Ttrip:
       # Calculate the vapor pressure for temperatures below the trip temperature
       return ptrip * (T/Ttrip)**((cpv-cvs)/rgasv) * math.exp((E0v + E0s -(cvv-cvs)*Ttrip)/rgasv * (1./Ttrip - 1./T))
   else:
       # Calculate the vapor pressure for temperatures above the trip temperature
       return ptrip * (T/Ttrip)**((cpv-cvl)/rgasv) * math.exp((E0v - (cvv-cvl)*Ttrip)/rgasv * (1./Ttrip - 1./T))

# Function to calculate the latent heat of vaporization at a given temperature
def Le(T):
   return (E0v + (cvv-cvl)*(T-Ttrip) + rgasv*T)

# Thermoregulatory parameters (used for heat exchange and regulation in the human body)
sigma = 5.67e-8  # Stefan-Boltzmann constant in W/m^2/K^4
epsilon = 0.97   # Emissivity of the surface (reflectivity of heat)
M = 83.6         # Mass of an average US adult in kg
H = 1.69         # Height of an average US adult in m
A = 0.202 * (M**0.425) * (H**0.725)  # DuBois formula for surface area in m^2
cpc = 3492.      # Specific heat capacity of the body core in J/kg/K
C = M * cpc / A  # Heat capacity of the body core in J/K
r = 124.         # Zf/Rf, used for calculating thermal resistance
Qr = 180.        # Metabolic rate per skin area in W/m^2
phi_salt = 0.9   # Vapor saturation pressure level for saline solution
Tc = 310.        # Core temperature in K (assumed)
Pc = phi_salt * pvstar(Tc)  # Core vapor pressure
L = Le(310.)     # Latent heat of vaporization at 310K
p = 1.013e5      # Atmospheric pressure in Pa
eta = 1.43e-6    # Inhaled mass / metabolic rate
Pa0 = 1.6e3      # Reference air vapor pressure in regions III, IV, V, VI

# Solar heat gain function
def Qsolar(mrt):
   # Calculate solar heat gain in W/m^2, based on mean radiant temperature (mrt)
   return ((mrt**4) * sigma * epsilon) / A

# Thermoregulatory functions for calculating heat loss and resistance
def Qv(Ta, Pa, Qx):
   # Respiratory heat loss in W/m^2 based on air temperature, air vapor pressure, and metabolic heat
   return eta * Qx * (cpa * (Tc - Ta) + L * rgasa / (p * rgasv) * (Pc - Pa))

def Zs(Rs):
   # Mass transfer resistance through skin in Pa m^2/W, based on skin resistance (Rs)
   return (52.1 if Rs == 0.0387 else 6.0e8 * Rs**5)

def Ra(Ts, Ta):
   # Heat transfer resistance through air, exposed skin part, in K m^2/W
   hc = 17.4
   phi_rad = 0.85
   hr = epsilon * phi_rad * sigma * (Ts**2 + Ta**2) * (Ts + Ta)
   return 1. / (hc + hr)

def Ra_bar(Tf, Ta):
   # Heat transfer resistance through air, clothed skin part, in K m^2/W
   hc = 11.6
   phi_rad = 0.79
   hr = epsilon * phi_rad * sigma * (Tf**2 + Ta**2) * (Tf + Ta)
   return 1. / (hc + hr)

def Ra_un(Ts, Ta):
   # Heat transfer resistance through air, when naked, in K m^2/W
   hc = 12.3
   phi_rad = 0.80
   hr = epsilon * phi_rad * sigma * (Ts**2 + Ta**2) * (Ts + Ta)
   return 1. / (hc + hr)

# Pre-calculated values for mass transfer resistance through air for different conditions
Za = 60.6 / 17.4    # Pa m^2/W for exposed skin
Za_bar = 60.6 / 11.6  # Pa m^2/W for clothed skin
Za_un = 60.6 / 12.3   # Pa m^2/W for naked skin

# Tolerance and maximum iteration for the root solver (used in calculations to find solutions to equations)
tol = 1e-7
tolT = 1e-8
maxIter = 300

# Given air temperature, relative humidity, and total heat gain, this function calculates various equilibrium variables
def find_eqvar(Ta, RH, Qmet, Qs=0):
    Pa = RH * pvstar(Ta)  # Calculate the air vapor pressure (Pa)
    Rs = 0.0387           # Heat transfer resistance through skin in m^2K/W (a constant value)
    phi = 0.84            # Fraction of the body covered by clothing (assumed constant)
    dTcdt = 0.            # Rate of change in core temperature (in K/s)
    
    # Calculate the mass transfer resistance between the body and the surroundings
    m = (Pc - Pa) / (Zs(Rs) + Za)       # Mass transfer resistance for exposed skin
    m_bar = (Pc - Pa) / (Zs(Rs) + Za_bar)  # Mass transfer resistance for clothed skin

    # Solve for the skin temperature (Ts) by solving the heat balance equation
    Ts = solve(lambda Ts: (Ts - Ta) / Ra(Ts, Ta) + (Pc - Pa) / (Zs(Rs) + Za) - (Tc - Ts) / Rs,
               max(0., min(Tc, Ta) - Rs * abs(m)), max(Tc, Ta) + Rs * abs(m), tol, maxIter)
    
    # Solve for the clothing temperature (Tf) using a similar approach
    Tf = solve(lambda Tf: (Tf - Ta) / Ra_bar(Tf, Ta) + (Pc - Pa) / (Zs(Rs) + Za_bar) - (Tc - Tf) / Rs,
               max(0., min(Tc, Ta) - Rs * abs(m_bar)), max(Tc, Ta) + Rs * abs(m_bar), tol, maxIter)

    # Calculate heat flux based on the conditions (for different regions)
    flux1 = (Qmet + Qs) - Qv(Ta, Pa, Qmet) - (1. - phi) * (Tc - Ts) / Rs  # Heat flux in Region I
    flux2 = (Qmet + Qs) - Qv(Ta, Pa, Qmet) - (1. - phi) * (Tc - Ts) / Rs - phi * (Tc - Tf) / Rs  # Heat flux in Region II & III

    if flux1 <= 0.:  # Region I (no clothed, all exposed skin)
        eqvar_name = "phi"  # The variable for Region I is the clothing fraction phi
        phi = 1. - ((Qmet + Qs) - Qv(Ta, Pa, Qmet)) * Rs / (Tc - Ts)  # Calculate the clothing fraction
        Rf = float('inf')  # Infinite thermal resistance (since there is no clothing)
    elif flux2 <= 0.:  # Region II & III (clothed with different vapor pressures)
        eqvar_name = "Rf"  # The variable for Region II & III is clothing resistance Rf
        Ts_bar = Tc - ((Qmet + Qs) - Qv(Ta, Pa, Qmet)) * Rs / phi + (1. / phi - 1.) * (Tc - Ts)  # Effective temperature at the skin
        Tf = solve(lambda Tf: (Tf - Ta) / Ra_bar(Tf, Ta) + (Pc - Pa) * (Tf - Ta) / ((Zs(Rs) + Za_bar) * (Tf - Ta) + r * Ra_bar(Tf, Ta) * (Ts_bar - Tf)) - (Tc - Ts_bar) / Rs,
                   Ta, Ts_bar, tol, maxIter)
        Rf = Ra_bar(Tf, Ta) * (Ts_bar - Tf) / (Tf - Ta)  # Calculate clothing resistance Rf
    else:  # Region IV, V, VI (naked with thermal resistance through skin)
        Rf = 0.  # No resistance for naked skin (Region IV)
        flux3 = (Qmet + Qs) - eta * (Qmet + Qs) * (cpa * (Tc - Ta) + ((L * rgasa) / (rgasv * p)) * (Pc - Pa)) - 0.80 * epsilon * sigma * (Tc ** 4 - Ta ** 4) - 12.3 * (Tc - Ta) - ((Pc - Pa) / Za_un)

        if (flux3 < 0.):  # Region IV & V (sweating and naked conditions)
            Ts = solve(lambda Ts: (Ts - Ta) / Ra_un(Ts, Ta) + (Pc - Pa) / (Zs((Tc - Ts) / ((Qmet + Qs) - Qv(Ta, Pa, Qmet))) + Za_un) - ((Qmet + Qs) - Qv(Ta, Pa, Qmet)), 0., Tc, tol, maxIter)
            Rs = (Tc - Ts) / ((Qmet + Qs) - Qv(Ta, Pa, Qmet))  # Calculate skin resistance Rs
            eqvar_name = "Rs"
            Ps = Pc - (Pc - Pa) * Zs(Rs) / (Zs(Rs) + Za_un)
            if Ps > phi_salt * pvstar(Ts):  # Region V (sweating with clothing)
                Ts = solve(lambda Ts: (Ts - Ta) / Ra_un(Ts, Ta) + (phi_salt * pvstar(Ts) - Pa) / Za_un - ((Qmet + Qs) - Qv(Ta, Pa, Qmet)), 0., Tc, tol, maxIter)
                Rs = (Tc - Ts) / ((Qmet + Qs) - Qv(Ta, Pa, Qmet))
                eqvar_name = "Rs*"
                if Rs < 0.004:  # Force into Region VI if skin resistance goes too low
                    Rs = 0.004  # Ensure Rs doesn't go below 0.004
                    eqvar_name = "dTcdt"
                    dTcdt = (1. / C) * flux3  # Core temperature change rate in Region VI
        else:  # Region VI (warming up)
            Rs = 0.004  # Skin resistance is minimum in this region
            eqvar_name = "dTcdt"
            dTcdt = (1. / C) * flux3  # Core temperature change rate

    return [eqvar_name, phi, Rf, Rs, dTcdt]  # Return the calculated values

def f_dTcdt(T, eqvar):
        return find_eqvar(T, Pa0 / pvstar(T), Qr, 0.)[4] - eqvar

from scipy.optimize import minimize_scalar
import numpy as np

def auto_bracket_root(f, Tmin=270, Tmax=1000, N=5000):
    Tvals = np.linspace(Tmin, Tmax, N)
    fvals = [f(T) for T in Tvals]
    for i in range(N - 1):
        if np.sign(fvals[i]) != np.sign(fvals[i + 1]):
            return Tvals[i], Tvals[i + 1]
    raise RuntimeError("No root found during bracketing.")

def smart_bracket(f, a, b, max_nudges=3, nudge_size=5):
    fa = f(a)
    fb = f(b)
    if np.sign(fa) != np.sign(fb):
        return a, b
    for _ in range(max_nudges):
        a_new = a + nudge_size
        b_new = b - nudge_size
        if np.sign(f(a_new)) != np.sign(fb):
            return a_new, b
        if np.sign(fa) != np.sign(f(b_new)):
            return a, b_new
        a, b = a_new, b_new
    raise RuntimeError("Unable to find valid bracket after nudging.")

def solve_bisection(f, a, b, tol=1e-8, maxIter=1000):
    fa = f(a)
    fb = f(b)
    if fa * fb > 0:
        raise ValueError("Bisection failed: endpoints have same sign.")
    for i in range(maxIter):
        c = (a + b) / 2
        fc = f(c)
        if np.sign(fc) == np.sign(fa):
            a, fa = c, fc
        else:
            b, fb = c, fc
        if abs(a - b) < tol:
            return c
    raise RuntimeError("Max iterations reached in bisection.")

def solve_advanced(f, x1=270, x2=1000, tol=1e-8):
    result = minimize_scalar(lambda T: f(T)**2, bounds=(x1, x2), method='bounded', options={'xatol': tol})
    if result.success and abs(f(result.x)) < 1e-3:
        return result.x
    else:
        raise RuntimeError("Advanced minimization failed.")

def safe_solve(f, Tmin=270, Tmax=1000, tol=1e-8, maxIter=1000):
    try:
        a, b = auto_bracket_root(f, Tmin, Tmax, N=5000)
        a, b = smart_bracket(f, a, b)
        return solve_bisection(f, a, b, tol, maxIter)
    except Exception as e:
        print(f"[safe_solve] Falling back to minimization: {e}")
        try:
            return solve_advanced(f, Tmin, Tmax, tol)
        except Exception as e2:
            print(f"[safe_solve] Minimization also failed: {e2}")
            return np.nan


from scipy.optimize import minimize_scalar

def solve_powell(f, x1=270, x2=1000, tol=1e-6, method='bounded'):
    result = minimize_scalar(
        lambda T: abs(f(T)),  # Minimize |f(T)| to get as close to zero as possible
        bounds=(x1, x2),
        method=method,
        options={"xatol": tol}
    )

    if result.success:
        return result.x
    else:
        raise RuntimeError(f"Minimization failed: {result.message}")


# Given the equilibrium variable, find the corresponding heat index
def find_T(eqvar_name, eqvar):
    if eqvar_name == "phi":
        # Solve for temperature in Region I using the clothing fraction phi
        T = solve(lambda T: find_eqvar(T, 1., Qr, 0.)[1] - eqvar, 0., 400., tolT, maxIter)
        region = 'I'
        return T, region
    elif eqvar_name == "Rf":
        # Solve for temperature in Regions II and III based on clothing resistance Rf
        T = solve(lambda T: find_eqvar(T, min(1., Pa0 / pvstar(T)), Qr, 0.)[2] - eqvar, 230., 500., tolT, maxIter)
        region = 'II' if Pa0 > pvstar(T) else 'III'
        return T, region
    elif eqvar_name == "Rs" or eqvar_name == "Rs*":
        # Solve for temperature in Regions IV and V based on skin resistance Rs
        # T = solve(lambda T: find_eqvar(T, Pa0 / pvstar(T), Qr, 0.)[3] - eqvar, 330., 500., tolT, maxIter)
        f = lambda T: find_eqvar(T, Pa0 / pvstar(T), Qr, 0)[3] - eqvar
        T = safe_solve(f, Tmin=295, Tmax=500, tol=1e-8)
        region = 'IV' if eqvar_name == "Rs" else 'V'
        return T, region
    else:
        # Solve for temperature in Region VI (where core temperature is warming up)
        # f = lambda T: f_dTcdt(T, eqvar)
        # a, b = auto_bracket_root(f, N=2000)
        # T = solve(lambda T: find_eqvar(T, Pa0 / pvstar(T), Qr, 0.)[4] - eqvar, 330., 1000., tolT, maxIter)
        # f = lambda T: find_eqvar(T, Pa0 / pvstar(T), Qr, 0.)[4] - eqvar
        # T = solve_powell(f, 290, 1000, tol=tolT)
        # f = lambda T: find_eqvar(T, Pa0 / pvstar(T), Qr, 0.)[4] - eqvar
        # T = solve_advanced(f, x1=330, x2=1000, tol=1e-8)
        f = lambda T: find_eqvar(T, Pa0 / pvstar(T), Qr, 0.)[4] - eqvar
        try:
            a, b = auto_bracket_root(f, 330, 400, N=1000)
            a, b = smart_bracket(f, a, b)  # 👈 NEW
            T = solve(f, a, b, tolT, maxIter)
        except SystemExit:
            T = solve_advanced(f, 330, 400, tolT)
        region = 'VI'
        return T, region

    # return T, region  # Return the temperature and the corresponding region

# Combining the two functions find_eqvar and find_T to compute the modified heat index
def modifiedheatindex(Ta, RH, Qmet, mrt, show_info=False):
    Qs = Qsolar(float(mrt))  # Solar heat gain in the body (W/m^2)
    dic = {"phi": 1, "Rf": 2, "Rs": 3, "Rs*": 3, "dTcdt": 4}  # Dictionary to map variable names to indices
    eqvars = find_eqvar(Ta, RH, Qmet, Qs)  # Find the equilibrium variables
    eqvar = eqvars[dic[eqvars[0]]]
    T, region = find_T(eqvars[0], eqvar)  # Find the temperature and region based on eqvar
    if Ta == 0.: T = 0.  # If air temperature is 0, set heat index to 0

    # Print the results if show_info flag is True
    if show_info:
        if region == 'I':
            print("Region I, covering (variable phi)")
            print("Clothing fraction is " + str(round(eqvars[1], 3)))
        elif region == 'II':
            print("Region II, clothed (variable Rf, Pa = pvstar)")
            print("Clothing thickness is " + str(round((eqvars[2] / 16.7) * 100., 3)) + " cm")
        elif region == 'III':
            print("Region III, clothed (variable Rf, Pa = pref)")
            print("Clothing thickness is " + str(round((eqvars[2] / 16.7) * 100., 3)) + " cm")
        elif region == 'IV':
            # Calculations for Region IV (naked with thermal resistance through skin)
            kmin = 5.28  # W/K/m^2, conductance of tissue
            rho = 1.0e3  # kg/m^3, density of blood
            c = 4184.  # J/kg/K, specific heat of blood
            print("Region IV, naked (variable Rs, ps < phi_salt * pvstar)")
            print("Blood flow is " + str(round(((1. / eqvars[3] - kmin) * A / (rho * c)) * 1000. * 60., 3)) + " l/min")
        elif region == 'V':
            # Same as Region IV, but with sweating conditions (Region V)
            kmin = 5.28  # W/K/m^2, conductance of tissue
            rho = 1.0e3  # kg/m^3, density of blood
            c = 4184.  # J/kg/K, specific heat of blood
            print("Region V, naked dripping sweat (variable Rs, ps = phi_salt * pvstar)")
            print("Blood flow is " + str(round(((1. / eqvars[3] - kmin) * A / (rho * c)) * 1000. * 60., 3)) + " l/min")
        else:
            print("Region VI, warming up (dTc/dt > 0)")
            print("dTc/dt = " + str(round(eqvars[4] * 3600., 6)) + " K/hour")

    return T  # Return the calculated temperature (heat index)
from scipy.optimize import minimize_scalar

# def solve_advanced(f, x1=270, x2=1000, tol=1e-8):
#     """
#     Minimize |f(T)| over [x1, x2] to find the root or closest approximation.
#     """
#     result = minimize_scalar(
#         lambda T: abs(f(T)),
#         bounds=(x1, x2),
#         method='bounded',
#         options={'xatol': tol}
#     )

#     if result.success:
#         return result.x
#     else:
#         raise RuntimeError(f"[solve_advanced] Minimization failed: {result.message}")
def smart_bracket(f, a, b, max_nudges=3, nudge_size=5):
    """
    Try to fix a bad bracket by nudging the endpoints.
    """
    fa = f(a)
    fb = f(b)

    if np.sign(fa) != np.sign(fb):
        return a, b  # Good bracket already

    for _ in range(max_nudges):
        # Try nudging lower endpoint
        a_new = a + nudge_size
        fa_new = f(a_new)
        if np.sign(fa_new) != np.sign(fb):
            return a_new, b

        # Try nudging upper endpoint
        b_new = b - nudge_size
        fb_new = f(b_new)
        if np.sign(fa) != np.sign(fb_new):
            return a, b_new

        # Update a and b for next loop
        a, fa = a_new, fa_new
        b, fb = b_new, fb_new

    raise RuntimeError("Unable to find a valid bracket after nudging.")

# The following function is used to solve equations for root finding (e.g., finding temperature from given conditions)
def solve(f, x1, x2, tol, maxIter):
    a = x1
    b = x2
    fa = f(a)
    fb = f(b)
    
    # Ensure that the initial guesses for the root are valid
    if fa * fb > 0.:
        raise SystemExit('Wrong initial interval in the root solver')
        return None
    else:
        for i in range(maxIter):
            c = (a + b) / 2.
            fc = f(c)
            if fb * fc > 0.:
                b = c
                fb = fc
            else:
                a = c
                fa = fc
            if abs(a - b) < tol:  # Check if the solution has converged
                return c
            if i == maxIter - 1:  # If max iterations reached, raise an error
                raise SystemExit('Reaching maximum iteration in the root solver')
                return None
