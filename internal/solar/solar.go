package solar

import "math"

// ClearSkyGHI estimates the Global Horizontal Irradiance (W/m²) for a
// completely clear sky at the given latitude, longitude, and UTC time.
// Uses the Haurwitz (1945) empirical model. Returns 0 when the sun is at or
// below the effective horizon (cosine of zenith ≤ 0.017, i.e. < ~1° elevation).
func ClearSkyGHI(lat, lon float64, unixUTC int64) float64 {
	cosZ := CosSolarZenith(lat, lon, unixUTC)
	if cosZ < 0.017 { // sun effectively at or below horizon
		return 0
	}
	// Haurwitz model: empirically validated for typical clear-sky conditions.
	// Produces ~860 W/m² at zenith, falls smoothly toward 0 at the horizon.
	return 910.0 * cosZ * math.Exp(-0.057/cosZ)
}

// CloudCover returns the clearness index Kt and estimated cloud cover
// percentage for a measured GHI against a clear-sky GHI estimate.
//
// When clearSkyGHI is effectively zero (sun below horizon), Kt is 0 and
// cloudPct is -1 to signal "not computable at night."
//
// Piecewise mapping per the Kasten-Czeplak breakpoints:
//   Kt > 0.8         →  0–20 % cloud (clear)
//   0.3 ≤ Kt ≤ 0.8  →  20–70 % cloud (partly cloudy)
//   Kt < 0.3         →  70–100 % cloud (overcast)
func CloudCover(measuredGHI, clearSkyGHI float64) (kt, cloudPct float64) {
	if clearSkyGHI < 1.0 {
		return 0, -1 // nighttime / sun below horizon
	}
	kt = measuredGHI / clearSkyGHI
	if kt > 1 {
		kt = 1
	}
	if kt < 0 {
		kt = 0
	}
	switch {
	case kt >= 0.8:
		cloudPct = (1 - kt) / 0.2 * 20
	case kt >= 0.3:
		cloudPct = 20 + (0.8-kt)/0.5*50
	default:
		cloudPct = 70 + (0.3-kt)/0.3*30
	}
	return kt, cloudPct
}

// CosSolarZenith returns cos(solar zenith angle) for the given location and
// Unix UTC timestamp. Exported so tests can verify the intermediate value.
// Negative return means the sun is below the horizon.
func CosSolarZenith(lat, lon float64, unixUTC int64) float64 {
	latRad := lat * math.Pi / 180

	// Fractional day of year (1-based, continuous)
	// 86400 seconds per day; epoch day = unixUTC / 86400
	// Day of year within the Gregorian year:
	dayOfYear := dayOfYearFromUnix(unixUTC)
	B := 2 * math.Pi * (dayOfYear - 1) / 365.0

	// Solar declination — Spencer (1971) Fourier series, accurate to ±0.0006 rad
	decl := 0.006918 -
		0.399912*math.Cos(B) + 0.070257*math.Sin(B) -
		0.006758*math.Cos(2*B) + 0.000907*math.Sin(2*B) -
		0.002697*math.Cos(3*B) + 0.001480*math.Sin(3*B)

	// Equation of time (minutes)
	eot := 229.18 * (0.000075 +
		0.001868*math.Cos(B) - 0.032077*math.Sin(B) -
		0.014615*math.Cos(2*B) - 0.040890*math.Sin(2*B))

	// UTC hour as a decimal
	secondsInDay := unixUTC % 86400
	utcHour := float64(secondsInDay) / 3600.0

	// Apparent solar time (hours): shift UTC by longitude (15°/hour) and EoT
	solarTime := utcHour + lon/15.0 + eot/60.0

	// Hour angle (radians): 0 at solar noon, ±π at midnight
	hourAngle := (solarTime - 12.0) * math.Pi / 12.0

	// cos(zenith) = sin(φ)sin(δ) + cos(φ)cos(δ)cos(H)
	return math.Sin(latRad)*math.Sin(decl) +
		math.Cos(latRad)*math.Cos(decl)*math.Cos(hourAngle)
}

// dayOfYearFromUnix returns the day-of-year (1 = Jan 1) for a Unix UTC timestamp.
func dayOfYearFromUnix(unixUTC int64) float64 {
	// Days since Unix epoch (1970-01-01)
	days := float64(unixUTC) / 86400.0

	// Approximate year and day-of-year using Julian Day arithmetic.
	// Accurate to within ±1 day, which is sufficient for solar angle calculations.
	jd := days + 2440587.5 // Julian Day Number for Unix epoch
	// Meeus algorithm for Gregorian calendar
	z := int64(jd + 0.5)
	a := z
	if z >= 2299161 {
		alpha := int64((float64(z) - 1867216.25) / 36524.25)
		a = z + 1 + alpha - alpha/4
	}
	b := a + 1524
	c := int64((float64(b) - 122.1) / 365.25)
	d := int64(365.25 * float64(c))
	yearDay := b - d
	// d is now day-of-year within the year; months would require more work,
	// but we only need the day number 1-365 (366).
	// The Julian Day of Jan 1 of the same year:
	year := c - 4716
	if yearDay > 306 {
		year++
	}
	// Jan 1 of 'year' in Julian Days:
	y := year - 1
	jan1JD := int64(365*y) + y/4 - y/100 + y/400 + 1721426
	return float64(z-jan1JD) + 1
}
