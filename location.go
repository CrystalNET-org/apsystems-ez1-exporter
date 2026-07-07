package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type geocodeResult struct {
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
	Name        string  `json:"name"`
	CountryCode string  `json:"country_code"`
}

type geocodeResponse struct {
	Results []geocodeResult `json:"results"`
}

// geocodeCity resolves a "City" or "City,CC" string to coordinates via
// Open-Meteo's free geocoding API (no key required). Done once at startup
// only - nothing in this exporter depends on it being reachable afterwards.
func geocodeCity(city string) (latitude, longitude float64, err error) {
	name, countryCode, _ := strings.Cut(city, ",")
	name = strings.TrimSpace(name)
	countryCode = strings.TrimSpace(countryCode)

	u := "https://geocoding-api.open-meteo.com/v1/search?" + url.Values{
		"name":     {name},
		"count":    {"10"},
		"language": {"en"},
		"format":   {"json"},
	}.Encode()

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(u)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to request geocoding api: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("unexpected status %d from geocoding api", resp.StatusCode)
	}

	var parsed geocodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return 0, 0, fmt.Errorf("failed to decode geocoding api response: %w", err)
	}
	if len(parsed.Results) == 0 {
		return 0, 0, fmt.Errorf("no geocoding results for %q", city)
	}

	if countryCode != "" {
		for _, result := range parsed.Results {
			if strings.EqualFold(result.CountryCode, countryCode) {
				return result.Latitude, result.Longitude, nil
			}
		}
		return 0, 0, fmt.Errorf("no geocoding result for %q matched country code %q", name, countryCode)
	}

	return parsed.Results[0].Latitude, parsed.Results[0].Longitude, nil
}

func parseLatLong(raw string) (latitude, longitude float64, err error) {
	latStr, lonStr, found := strings.Cut(raw, ",")
	if !found {
		return 0, 0, fmt.Errorf("expected \"lat,long\", got %q", raw)
	}
	latitude, err = strconv.ParseFloat(strings.TrimSpace(latStr), 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid latitude in %q: %w", raw, err)
	}
	longitude, err = strconv.ParseFloat(strings.TrimSpace(lonStr), 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid longitude in %q: %w", raw, err)
	}
	return latitude, longitude, nil
}

// resolveLocation returns nil, nil if neither env var is set - the
// day/night-aware polling feature is entirely optional.
func resolveLocation(latLongEnv, cityEnv string) (*daylightWindow, error) {
	if latLongEnv != "" {
		lat, lon, err := parseLatLong(latLongEnv)
		if err != nil {
			return nil, fmt.Errorf("failed to parse lat/long: %w", err)
		}
		return newDaylightWindow(lat, lon), nil
	}
	if cityEnv != "" {
		lat, lon, err := geocodeCity(cityEnv)
		if err != nil {
			return nil, fmt.Errorf("failed to geocode city: %w", err)
		}
		return newDaylightWindow(lat, lon), nil
	}
	return nil, nil
}
