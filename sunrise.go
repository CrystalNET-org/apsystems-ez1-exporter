package main

import (
	"math"
	"sync"
	"time"
)

// civilTwilightElevation is the solar elevation angle (degrees) used to
// define "first light" / "no light left" - civil dawn/dusk - rather than
// the sun's disc actually touching the horizon (standard sunrise/sunset
// uses -0.833 degrees). At -6 degrees there's still enough ambient light
// to be useful, and a solar panel can still produce a trickle of power.
const civilTwilightElevation = -6.0

const j2000 = 2451545.0

// civilDawnDusk returns the civil dawn and civil dusk (both in UTC) for the
// given date at the given latitude/longitude, using the standard "sunrise
// equation" (see https://en.wikipedia.org/wiki/Sunrise_equation, cross
// checked against github.com/nathan-osman/go-sunrise) with the elevation
// angle swapped from the usual -0.833 degrees to -6 degrees.
// Returns ok=false if the sun never reaches that elevation on that day
// (permanent day or permanent night, relevant only at extreme latitudes).
func civilDawnDusk(latitude, longitude float64, date time.Time) (dawn, dusk time.Time, ok bool) {
	const deg = math.Pi / 180

	// anchored to solar noon (12:00 UTC), not midnight - the mean anomaly/
	// equation of center below are only valid relative to that anchor.
	noon := time.Date(date.Year(), date.Month(), date.Day(), 12, 0, 0, 0, time.UTC)
	meanSolarNoon := toJulianDay(noon) - longitude/360

	solarMeanAnomaly := math.Mod(357.5291+0.98560028*(meanSolarNoon-j2000), 360)
	if solarMeanAnomaly < 0 {
		solarMeanAnomaly += 360
	}
	m := solarMeanAnomaly * deg
	equationOfCenter := 1.9148*math.Sin(m) + 0.0200*math.Sin(2*m) + 0.0003*math.Sin(3*m)

	argumentOfPerihelion := 102.93005 + 0.3179526*(meanSolarNoon-j2000)/36525
	eclipticLongitude := math.Mod(solarMeanAnomaly+equationOfCenter+180+argumentOfPerihelion, 360)
	lambda := eclipticLongitude * deg

	solarTransit := meanSolarNoon + 0.0053*math.Sin(m) - 0.0069*math.Sin(2*lambda)

	declination := math.Asin(math.Sin(lambda) * math.Sin(23.4397*deg))

	numerator := math.Sin(civilTwilightElevation*deg) - math.Sin(latitude*deg)*math.Sin(declination)
	denominator := math.Cos(latitude*deg) * math.Cos(declination)
	cosHourAngle := numerator / denominator
	if cosHourAngle > 1 || cosHourAngle < -1 {
		// sun never reaches civilTwilightElevation today at this latitude
		return time.Time{}, time.Time{}, false
	}
	hourAngle := math.Acos(cosHourAngle) / deg

	frac := hourAngle / 360
	return fromJulianDay(solarTransit - frac), fromJulianDay(solarTransit + frac), true
}

func toJulianDay(t time.Time) float64 {
	t = t.UTC()
	return float64(t.Unix())/86400.0 + 2440587.5
}

func fromJulianDay(jd float64) time.Time {
	return time.Unix(int64((jd-2440587.5)*86400.0), 0).UTC()
}

// daylightWindow tracks the current day's civil dawn/dusk and recomputes
// them once per calendar day (UTC) rather than on every check.
type daylightWindow struct {
	mu        sync.Mutex
	latitude  float64
	longitude float64
	forDate   time.Time
	dawn      time.Time
	dusk      time.Time
	always    bool // true if the sun never reaches civil twilight elevation (e.g. polar day/night)
}

func newDaylightWindow(latitude, longitude float64) *daylightWindow {
	return &daylightWindow{latitude: latitude, longitude: longitude}
}

func (d *daylightWindow) isDaylight(now time.Time) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	today := now.UTC().Truncate(24 * time.Hour)
	if !today.Equal(d.forDate) {
		dawn, dusk, ok := civilDawnDusk(d.latitude, d.longitude, today)
		d.forDate = today
		d.dawn, d.dusk, d.always = dawn, dusk, !ok
	}

	if d.always {
		return true
	}
	return !now.Before(d.dawn) && now.Before(d.dusk)
}
