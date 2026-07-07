package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type deviceInfo struct {
	DeviceID string `json:"deviceId"`
	DevVer   string `json:"devVer"`
	SSID     string `json:"ssid"`
	IPAddr   string `json:"ipAddr"`
	MinPower string `json:"minPower"`
	MaxPower string `json:"maxPower"`
}

type outputData struct {
	P1  float64 `json:"p1"`
	E1  float64 `json:"e1"`
	Te1 float64 `json:"te1"`
	P2  float64 `json:"p2"`
	E2  float64 `json:"e2"`
	Te2 float64 `json:"te2"`
}

type maxPowerData struct {
	MaxPower string `json:"maxPower"`
}

type alarmData struct {
	OG    string `json:"og"`
	Isce1 string `json:"isce1"`
	Isce2 string `json:"isce2"`
	OE    string `json:"oe"`
}

type onOffData struct {
	Status string `json:"status"`
}

type apiResponse[T any] struct {
	Data    T      `json:"data"`
	Message string `json:"message"`
}

type ez1Client struct {
	baseURL string
	client  *http.Client
}

func newEz1Client(baseURL string, timeout time.Duration) *ez1Client {
	return &ez1Client{baseURL: baseURL, client: &http.Client{Timeout: timeout}}
}

func fetch[T any](c *ez1Client, path string) (T, error) {
	var zero T
	u, err := url.JoinPath(c.baseURL, path)
	if err != nil {
		return zero, fmt.Errorf("failed to build url for %s: %w", path, err)
	}
	resp, err := c.client.Get(u)
	if err != nil {
		return zero, fmt.Errorf("failed to request %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return zero, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, path)
	}
	var parsed apiResponse[T]
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return zero, fmt.Errorf("failed to decode response from %s: %w", path, err)
	}
	if parsed.Message != "SUCCESS" {
		return zero, fmt.Errorf("device reported failure for %s: %s", path, parsed.Message)
	}
	return parsed.Data, nil
}

// outputSnapshot holds the latest polled high-resolution power/energy data
// (/getOutputData), refreshed on its own ticker independent of statusSnapshot.
//
// Momentary power (P1/P2) is zeroed out whenever we don't have a fresh
// confirmed reading (poll failure, or outside the daylight window) - it can
// genuinely swing to zero at any moment, so serving a stale nonzero value
// would be actively misleading. The energy counters (E1/E2/Te1/Te2) are
// cumulative and only ever move forward, so they keep their last known-good
// value instead of being zeroed, which would show up as a false dip.
type outputSnapshot struct {
	mu       sync.RWMutex
	data     outputData
	ok       bool
	daylight bool
	updated  time.Time
}

func (s *outputSnapshot) setSuccess(data outputData, daylight bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data, s.ok, s.daylight, s.updated = data, true, daylight, time.Now()
}

// setPowerUnknown records a poll failure or a night-time skip: P1/P2 are
// zeroed, but the previously recorded energy counters are left untouched.
func (s *outputSnapshot) setPowerUnknown(ok, daylight bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.P1, s.data.P2 = 0, 0
	s.ok, s.daylight, s.updated = ok, daylight, time.Now()
}

func (s *outputSnapshot) get() (outputData, bool, bool, time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data, s.ok, s.daylight, s.updated
}

// statusSnapshot holds the latest polled low-resolution data - device info,
// configured max power, alarms and on/off state - which changes rarely and
// so is refreshed on a much longer interval than outputSnapshot.
type statusSnapshot struct {
	mu       sync.RWMutex
	info     deviceInfo
	maxPower maxPowerData
	alarm    alarmData
	onOff    onOffData
	ok       bool
	updated  time.Time
}

func (s *statusSnapshot) set(info deviceInfo, maxPower maxPowerData, alarm alarmData, onOff onOffData, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.info, s.maxPower, s.alarm, s.onOff, s.ok, s.updated = info, maxPower, alarm, onOff, ok, time.Now()
}

func (s *statusSnapshot) get() (deviceInfo, maxPowerData, alarmData, onOffData, bool, time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.info, s.maxPower, s.alarm, s.onOff, s.ok, s.updated
}

// pollEvery runs poll immediately, then again on every tick of interval,
// until ctx is cancelled.
func pollEvery(ctx context.Context, interval time.Duration, poll func()) {
	poll()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			poll()
		}
	}
}

func pollOutput(ctx context.Context, client *ez1Client, interval time.Duration, snap *outputSnapshot, daylight *daylightWindow) {
	pollEvery(ctx, interval, func() {
		if daylight != nil && !daylight.isDaylight(time.Now()) {
			// outside the configured daylight window - the device won't be
			// generating anything, so skip hitting it. This is a confirmed
			// zero (ok=true), not an unknown/failed one.
			snap.setPowerUnknown(true, false)
			return
		}
		data, err := fetch[outputData](client, "/getOutputData")
		if err != nil {
			slog.Error("failed to poll output data", "error", err)
			snap.setPowerUnknown(false, true)
			return
		}
		snap.setSuccess(data, true)
	})
}

func pollStatus(ctx context.Context, client *ez1Client, interval time.Duration, snap *statusSnapshot) {
	pollEvery(ctx, interval, func() {
		var wg sync.WaitGroup
		var info deviceInfo
		var maxPower maxPowerData
		var alarm alarmData
		var onOff onOffData
		var infoErr, maxPowerErr, alarmErr, onOffErr error

		wg.Add(4)
		go func() { defer wg.Done(); info, infoErr = fetch[deviceInfo](client, "/getDeviceInfo") }()
		go func() { defer wg.Done(); maxPower, maxPowerErr = fetch[maxPowerData](client, "/getMaxPower") }()
		go func() { defer wg.Done(); alarm, alarmErr = fetch[alarmData](client, "/getAlarm") }()
		go func() { defer wg.Done(); onOff, onOffErr = fetch[onOffData](client, "/getOnOff") }()
		wg.Wait()

		if infoErr != nil || maxPowerErr != nil || alarmErr != nil || onOffErr != nil {
			slog.Error("failed to poll status data",
				"infoErr", infoErr, "maxPowerErr", maxPowerErr, "alarmErr", alarmErr, "onOffErr", onOffErr)
			snap.set(deviceInfo{}, maxPowerData{}, alarmData{}, onOffData{}, false)
			return
		}
		snap.set(info, maxPower, alarm, onOff, true)
	})
}

// collector implements prometheus.Collector. It only ever reads from the
// two snapshots - all HTTP calls to the device happen in the background
// pollers on their own schedules, so a Prometheus scrape is always
// instant regardless of how slow the device's own HTTP server is.
type collector struct {
	output *outputSnapshot
	status *statusSnapshot

	outputUp             *prometheus.Desc
	outputLastUpdate     *prometheus.Desc
	daylight             *prometheus.Desc
	statusUp             *prometheus.Desc
	statusLastUpdate     *prometheus.Desc
	info                 *prometheus.Desc
	powerWatts           *prometheus.Desc
	energySinceStartup   *prometheus.Desc
	energyLifetime       *prometheus.Desc
	powerLimitMin        *prometheus.Desc
	powerLimitMax        *prometheus.Desc
	powerLimitConfigured *prometheus.Desc
	alarm                *prometheus.Desc
	deviceOn             *prometheus.Desc
}

func newCollector(output *outputSnapshot, status *statusSnapshot) *collector {
	return &collector{
		output:               output,
		status:               status,
		outputUp:             prometheus.NewDesc("ez1_output_up", "Whether the last poll of /getOutputData succeeded (1) or not (0); always 1 outside the configured daylight window, since no poll is attempted.", nil, nil),
		outputLastUpdate:     prometheus.NewDesc("ez1_output_last_update_timestamp_seconds", "Unix timestamp of the last /getOutputData poll attempt (or daylight-window check).", nil, nil),
		daylight:             prometheus.NewDesc("ez1_daylight", "Whether the current time is within the configured daylight (civil dawn to civil dusk) window. Always 1 if no location is configured.", nil, nil),
		statusUp:             prometheus.NewDesc("ez1_status_up", "Whether the last poll of device info/max power/alarm/on-off succeeded (1) or not (0).", nil, nil),
		statusLastUpdate:     prometheus.NewDesc("ez1_status_last_update_timestamp_seconds", "Unix timestamp of the last device info/max power/alarm/on-off poll attempt.", nil, nil),
		info:                 prometheus.NewDesc("ez1_info", "Static EZ1 device information.", []string{"device_id", "dev_ver", "ssid", "ip_addr"}, nil),
		powerWatts:           prometheus.NewDesc("ez1_power_watts", "Current output power, per channel.", []string{"channel"}, nil),
		energySinceStartup:   prometheus.NewDesc("ez1_energy_since_startup_kwh", "Energy generated since the inverter last started up, per channel.", []string{"channel"}, nil),
		energyLifetime:       prometheus.NewDesc("ez1_energy_lifetime_kwh", "Lifetime energy generated, per channel.", []string{"channel"}, nil),
		powerLimitMin:        prometheus.NewDesc("ez1_power_limit_min_watts", "Minimum max-power the device can be configured to.", nil, nil),
		powerLimitMax:        prometheus.NewDesc("ez1_power_limit_max_watts", "Maximum max-power the device can be configured to (hardware ceiling).", nil, nil),
		powerLimitConfigured: prometheus.NewDesc("ez1_power_limit_configured_watts", "Currently configured max-power setting.", nil, nil),
		alarm:                prometheus.NewDesc("ez1_alarm", "EZ1 alarm state (1 = active, 0 = normal), by alarm type.", []string{"type"}, nil),
		deviceOn:             prometheus.NewDesc("ez1_device_on", "Whether the device is currently switched on (1) or off (0).", nil, nil),
	}
}

func (col *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- col.outputUp
	ch <- col.outputLastUpdate
	ch <- col.daylight
	ch <- col.statusUp
	ch <- col.statusLastUpdate
	ch <- col.info
	ch <- col.powerWatts
	ch <- col.energySinceStartup
	ch <- col.energyLifetime
	ch <- col.powerLimitMin
	ch <- col.powerLimitMax
	ch <- col.powerLimitConfigured
	ch <- col.alarm
	ch <- col.deviceOn
}

func (col *collector) Collect(ch chan<- prometheus.Metric) {
	output, outputOK, daylight, outputUpdated := col.output.get()
	info, maxPower, alarm, onOff, statusOK, statusUpdated := col.status.get()

	ch <- prometheus.MustNewConstMetric(col.outputUp, prometheus.GaugeValue, boolToFloat(outputOK))
	ch <- prometheus.MustNewConstMetric(col.outputLastUpdate, prometheus.GaugeValue, float64(outputUpdated.Unix()))
	ch <- prometheus.MustNewConstMetric(col.daylight, prometheus.GaugeValue, boolToFloat(daylight))
	ch <- prometheus.MustNewConstMetric(col.statusUp, prometheus.GaugeValue, boolToFloat(statusOK))
	ch <- prometheus.MustNewConstMetric(col.statusLastUpdate, prometheus.GaugeValue, float64(statusUpdated.Unix()))

	// always emitted, never omitted: power is explicitly zeroed by the
	// poller on both failure and night-time skip (see outputSnapshot's
	// doc comment), so there's no stale power reading to worry about here.
	// The energy counters below deliberately keep their last known-good
	// value instead of being zeroed alongside power.
	ch <- prometheus.MustNewConstMetric(col.powerWatts, prometheus.GaugeValue, output.P1, "1")
	ch <- prometheus.MustNewConstMetric(col.powerWatts, prometheus.GaugeValue, output.P2, "2")
	ch <- prometheus.MustNewConstMetric(col.energySinceStartup, prometheus.GaugeValue, output.E1, "1")
	ch <- prometheus.MustNewConstMetric(col.energySinceStartup, prometheus.GaugeValue, output.E2, "2")
	ch <- prometheus.MustNewConstMetric(col.energyLifetime, prometheus.GaugeValue, output.Te1, "1")
	ch <- prometheus.MustNewConstMetric(col.energyLifetime, prometheus.GaugeValue, output.Te2, "2")

	if !statusOK {
		return
	}

	ch <- prometheus.MustNewConstMetric(col.info, prometheus.GaugeValue, 1, info.DeviceID, info.DevVer, info.SSID, info.IPAddr)

	if v, err := strconv.ParseFloat(info.MinPower, 64); err == nil {
		ch <- prometheus.MustNewConstMetric(col.powerLimitMin, prometheus.GaugeValue, v)
	}
	if v, err := strconv.ParseFloat(info.MaxPower, 64); err == nil {
		ch <- prometheus.MustNewConstMetric(col.powerLimitMax, prometheus.GaugeValue, v)
	}
	if v, err := strconv.ParseFloat(maxPower.MaxPower, 64); err == nil {
		ch <- prometheus.MustNewConstMetric(col.powerLimitConfigured, prometheus.GaugeValue, v)
	}

	ch <- prometheus.MustNewConstMetric(col.alarm, prometheus.GaugeValue, alarmFlag(alarm.OG), "off_grid")
	ch <- prometheus.MustNewConstMetric(col.alarm, prometheus.GaugeValue, alarmFlag(alarm.OE), "output_fault")
	ch <- prometheus.MustNewConstMetric(col.alarm, prometheus.GaugeValue, alarmFlag(alarm.Isce1), "dc1_short_circuit")
	ch <- prometheus.MustNewConstMetric(col.alarm, prometheus.GaugeValue, alarmFlag(alarm.Isce2), "dc2_short_circuit")

	// API reports status "0" for On / "1" for Off - inverted here so the
	// exported metric follows the usual 1=on convention.
	deviceOn := 0.0
	if onOff.Status == "0" {
		deviceOn = 1.0
	}
	ch <- prometheus.MustNewConstMetric(col.deviceOn, prometheus.GaugeValue, deviceOn)
}

func alarmFlag(raw string) float64 {
	if raw == "1" {
		return 1
	}
	return 0
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func envDuration(name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	parsed, err := time.ParseDuration(v)
	if err != nil {
		slog.Error("invalid duration, using default", "env", name, "value", v, "default", def)
		return def
	}
	return parsed
}

func main() {
	targetURL := os.Getenv("EZ1_TARGET_URL")
	if targetURL == "" {
		slog.Error("EZ1_TARGET_URL is required, e.g. http://192.168.1.50:8050")
		os.Exit(1)
	}

	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = ":8090"
	}

	requestTimeout := envDuration("EZ1_REQUEST_TIMEOUT", 5*time.Second)
	outputInterval := envDuration("EZ1_OUTPUT_INTERVAL", 10*time.Second)
	statusInterval := envDuration("EZ1_STATUS_INTERVAL", 5*time.Minute)

	// day/night-aware output polling is entirely optional - if neither is
	// set, daylight is nil and pollOutput polls around the clock as before.
	daylight, err := resolveLocation(os.Getenv("EZ1_LOCATION_LATLONG"), os.Getenv("EZ1_LOCATION_CITY"))
	if err != nil {
		slog.Error("failed to resolve EZ1_LOCATION_LATLONG/EZ1_LOCATION_CITY", "error", err)
		os.Exit(1)
	}

	client := newEz1Client(targetURL, requestTimeout)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	output := &outputSnapshot{}
	status := &statusSnapshot{}
	go pollOutput(ctx, client, outputInterval, output, daylight)
	go pollStatus(ctx, client, statusInterval, status)

	registry := prometheus.NewRegistry()
	registry.MustRegister(newCollector(output, status))

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{Addr: listenAddr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	slog.Info("starting apsystems-ez1-exporter",
		"listenAddr", listenAddr, "targetURL", targetURL,
		"outputInterval", outputInterval, "statusInterval", statusInterval)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
