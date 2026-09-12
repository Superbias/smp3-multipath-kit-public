package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	accountingComplete        = "COMPLETE"
	accountingPartial         = "PARTIAL"
	accountingUnknown         = "UNKNOWN"
	accountingBucketRetention = 31 * 24 * time.Hour
)

type volumeTotals struct {
	Leg0CarrierBytes uint64 `json:"leg0_carrier_bytes"`
	Leg1CarrierBytes uint64 `json:"leg1_carrier_bytes"`
	Leg0UsefulBytes  uint64 `json:"leg0_useful_bytes"`
	Leg1UsefulBytes  uint64 `json:"leg1_useful_bytes"`

	NativeLeg0CarrierBytes     uint64 `json:"native_leg0_carrier_bytes"`
	NativeLeg1CarrierBytes     uint64 `json:"native_leg1_carrier_bytes"`
	StandaloneLeg0CarrierBytes uint64 `json:"standalone_leg0_carrier_bytes"`
	StandaloneLeg1CarrierBytes uint64 `json:"standalone_leg1_carrier_bytes"`
	NativeLeg0UsefulBytes      uint64 `json:"native_leg0_useful_bytes"`
	NativeLeg1UsefulBytes      uint64 `json:"native_leg1_useful_bytes"`
	StandaloneLeg0UsefulBytes  uint64 `json:"standalone_leg0_useful_bytes"`
	StandaloneLeg1UsefulBytes  uint64 `json:"standalone_leg1_useful_bytes"`
}

// TrafficVolume is the browser-facing cumulative byte view. All raw fields
// remain uint64 byte counters; display fields are convenience values only.
type TrafficVolume struct {
	Leg0CarrierBytes         uint64   `json:"leg0_carrier_bytes"`
	Leg1CarrierBytes         uint64   `json:"leg1_carrier_bytes"`
	CombinedCarrierBytes     uint64   `json:"combined_carrier_bytes"`
	Leg0UsefulBytes          uint64   `json:"leg0_useful_bytes"`
	Leg1UsefulBytes          uint64   `json:"leg1_useful_bytes"`
	CombinedUsefulBytes      uint64   `json:"combined_useful_bytes"`
	NativeCarrierBytes       uint64   `json:"native_carrier_bytes"`
	StandaloneCarrierBytes   uint64   `json:"standalone_carrier_bytes"`
	NativeUsefulBytes        uint64   `json:"native_useful_bytes"`
	StandaloneUsefulBytes    uint64   `json:"standalone_useful_bytes"`
	UsefulCarrierRatio       *float64 `json:"useful_carrier_ratio"`
	Leg0CarrierDisplay       string   `json:"leg0_carrier_display"`
	Leg1CarrierDisplay       string   `json:"leg1_carrier_display"`
	CombinedCarrierDisplay   string   `json:"combined_carrier_display"`
	Leg0UsefulDisplay        string   `json:"leg0_useful_display"`
	Leg1UsefulDisplay        string   `json:"leg1_useful_display"`
	CombinedUsefulDisplay    string   `json:"combined_useful_display"`
	NativeCarrierDisplay     string   `json:"native_carrier_display"`
	StandaloneCarrierDisplay string   `json:"standalone_carrier_display"`
	NativeUsefulDisplay      string   `json:"native_useful_display"`
	StandaloneUsefulDisplay  string   `json:"standalone_useful_display"`
}

type accountingGap struct {
	Start  time.Time `json:"gap_start"`
	End    time.Time `json:"gap_end"`
	Reason string    `json:"reason"`
}

type trafficBucket struct {
	Start  time.Time    `json:"start"`
	Totals volumeTotals `json:"totals"`
}

type persistedTrafficAccounting struct {
	Version      int             `json:"version"`
	Generation   string          `json:"generation"`
	LastSampleAt time.Time       `json:"last_sample_at"`
	Totals       volumeTotals    `json:"totals"`
	Buckets      []trafficBucket `json:"buckets"`
	Gaps         []accountingGap `json:"gaps"`
}

type accountingBaseline struct {
	DataSent uint64
	Useful   uint64
}

type accountingKey struct {
	Session    string
	Leg        int
	Generation uint64
	Role       string
}

type TrafficReport struct {
	Period                  string            `json:"period"`
	Timezone                string            `json:"timezone"`
	Status                  string            `json:"accounting_status"`
	LowerBound              bool              `json:"lower_bound"`
	GapSeconds              int64             `json:"gap_seconds"`
	Gaps                    []accountingGap   `json:"gaps"`
	Volume                  TrafficVolume     `json:"volume"`
	MetricSemantics         map[string]string `json:"metric_semantics"`
	SamplingIntervalMS      int64             `json:"sampling_interval_ms,omitempty"`
	SnapshotAgeMS           int64             `json:"snapshot_age_ms,omitempty"`
	RateWindowMS            int64             `json:"rate_window_ms,omitempty"`
	HistoryBucketResolution string            `json:"history_bucket_resolution,omitempty"`
	TrafficShareBasis       string            `json:"traffic_share_basis,omitempty"`
	LastTelemetryUpdate     any               `json:"last_telemetry_update,omitempty"`
}

type TrafficHistoryResponse struct {
	Resolution string          `json:"resolution"`
	Timezone   string          `json:"timezone"`
	Status     string          `json:"accounting_status"`
	Items      []trafficBucket `json:"items"`
}

type trafficAccounting struct {
	mu                sync.RWMutex
	path              string
	location          *time.Location
	persistInterval   time.Duration
	generation        string
	lastPersist       time.Time
	dirty             bool
	loaded            bool
	generationChanged bool

	lastSampleAt time.Time
	totals       volumeTotals
	buckets      map[time.Time]volumeTotals
	gaps         []accountingGap
	baselines    map[accountingKey]accountingBaseline
}

func newTrafficAccounting(path, timezone, generation string, interval time.Duration) (*trafficAccounting, error) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	location := time.UTC
	if strings.TrimSpace(timezone) != "" {
		loaded, err := time.LoadLocation(timezone)
		if err != nil {
			return nil, fmt.Errorf("load accounting timezone: %w", err)
		}
		location = loaded
	}
	a := &trafficAccounting{
		path:            path,
		location:        location,
		persistInterval: interval,
		generation:      generation,
		buckets:         make(map[time.Time]volumeTotals),
		baselines:       make(map[accountingKey]accountingBaseline),
	}
	if err := a.load(); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *trafficAccounting) load() error {
	if a.path == "" {
		a.loaded = true
		return nil
	}
	data, err := os.ReadFile(a.path)
	if errors.Is(err, os.ErrNotExist) {
		a.loaded = true
		return nil
	}
	if err != nil {
		return fmt.Errorf("read traffic accounting: %w", err)
	}
	var state persistedTrafficAccounting
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("decode traffic accounting: %w", err)
	}
	a.totals = state.Totals
	a.lastSampleAt = state.LastSampleAt
	a.gaps = append([]accountingGap(nil), state.Gaps...)
	for _, bucket := range state.Buckets {
		a.buckets[bucket.Start.UTC()] = bucket.Totals
	}
	if state.Generation != "" && state.Generation != a.generation && !state.LastSampleAt.IsZero() {
		a.gaps = append(a.gaps, accountingGap{Start: state.LastSampleAt, End: time.Now().UTC(), Reason: "server_generation_changed"})
		a.generationChanged = true
	}
	a.loaded = true
	return nil
}

func (a *trafficAccounting) Collect(snapshot TelemetrySnapshot) {
	if a == nil || snapshot.Timestamp.IsZero() {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.loaded || (!a.lastSampleAt.IsZero() && !snapshot.Timestamp.After(a.lastSampleAt)) {
		return
	}
	if !a.generationChanged && !a.lastSampleAt.IsZero() && a.generation != "" && snapshot.Timestamp.Sub(a.lastSampleAt) > a.persistInterval*2 {
		a.gaps = append(a.gaps, accountingGap{Start: a.lastSampleAt, End: snapshot.Timestamp, Reason: "missing_telemetry_samples"})
	}
	seen := make(map[accountingKey]struct{}, len(snapshot.Sessions)*2)
	for _, session := range snapshot.Sessions {
		for leg := range session.Legs {
			row := session.Legs[leg]
			key := accountingKey{Session: session.DisplaySessionID, Leg: leg, Generation: row.Generation, Role: row.IngressRole}
			seen[key] = struct{}{}
			previous, exists := a.baselines[key]
			if exists {
				carrierDelta := uint64(0)
				if row.DataSentBytes >= previous.DataSent {
					carrierDelta = row.DataSentBytes - previous.DataSent
				}
				usefulDelta := uint64(0)
				usefulValid := row.LogicalTxAckedBytes >= previous.Useful
				if usefulValid {
					usefulDelta = row.LogicalTxAckedBytes - previous.Useful
				}
				if carrierDelta > 0 || usefulDelta > 0 {
					a.addDelta(snapshot.Timestamp, leg, row.IngressRole, carrierDelta, usefulDelta, usefulValid)
				}
				if row.DataSentBytes < previous.DataSent || !usefulValid {
					// A generation/session reset establishes a fresh baseline. The
					// negative interval is intentionally not counted.
				}
			}
			a.baselines[key] = accountingBaseline{DataSent: row.DataSentBytes, Useful: row.LogicalTxAckedBytes}
		}
	}
	for key := range a.baselines {
		if _, exists := seen[key]; !exists {
			delete(a.baselines, key)
		}
	}
	a.lastSampleAt = snapshot.Timestamp
	a.generationChanged = false
	a.pruneLocked(snapshot.Timestamp)
	a.dirty = true
}

func (a *trafficAccounting) addDelta(at time.Time, leg int, role string, carrier, useful uint64, usefulValid bool) {
	if leg < 0 || leg > 1 {
		return
	}
	delta := volumeTotals{}
	if leg == 0 {
		delta.Leg0CarrierBytes = carrier
		if usefulValid {
			delta.Leg0UsefulBytes = useful
		}
	} else {
		delta.Leg1CarrierBytes = carrier
		if usefulValid {
			delta.Leg1UsefulBytes = useful
		}
	}
	switch role {
	case TelemetryIngressNative:
		if leg == 0 {
			delta.NativeLeg0CarrierBytes = carrier
			if usefulValid {
				delta.NativeLeg0UsefulBytes = useful
			}
		} else {
			delta.NativeLeg1CarrierBytes = carrier
			if usefulValid {
				delta.NativeLeg1UsefulBytes = useful
			}
		}
	case TelemetryIngressStandalone:
		if leg == 0 {
			delta.StandaloneLeg0CarrierBytes = carrier
			if usefulValid {
				delta.StandaloneLeg0UsefulBytes = useful
			}
		} else {
			delta.StandaloneLeg1CarrierBytes = carrier
			if usefulValid {
				delta.StandaloneLeg1UsefulBytes = useful
			}
		}
	}
	a.totals = addVolumeTotals(a.totals, delta)
	minute := localMinute(at, a.location)
	a.buckets[minute] = addVolumeTotals(a.buckets[minute], delta)
}

func addVolumeTotals(left, right volumeTotals) volumeTotals {
	left.Leg0CarrierBytes += right.Leg0CarrierBytes
	left.Leg1CarrierBytes += right.Leg1CarrierBytes
	left.Leg0UsefulBytes += right.Leg0UsefulBytes
	left.Leg1UsefulBytes += right.Leg1UsefulBytes
	left.NativeLeg0CarrierBytes += right.NativeLeg0CarrierBytes
	left.NativeLeg1CarrierBytes += right.NativeLeg1CarrierBytes
	left.StandaloneLeg0CarrierBytes += right.StandaloneLeg0CarrierBytes
	left.StandaloneLeg1CarrierBytes += right.StandaloneLeg1CarrierBytes
	left.NativeLeg0UsefulBytes += right.NativeLeg0UsefulBytes
	left.NativeLeg1UsefulBytes += right.NativeLeg1UsefulBytes
	left.StandaloneLeg0UsefulBytes += right.StandaloneLeg0UsefulBytes
	left.StandaloneLeg1UsefulBytes += right.StandaloneLeg1UsefulBytes
	return left
}

func localMinute(value time.Time, location *time.Location) time.Time {
	local := value.In(location)
	return time.Date(local.Year(), local.Month(), local.Day(), local.Hour(), local.Minute(), 0, 0, location).UTC()
}

func (a *trafficAccounting) pruneLocked(now time.Time) {
	cutoff := now.Add(-accountingBucketRetention)
	for start := range a.buckets {
		if start.Before(cutoff) {
			delete(a.buckets, start)
		}
	}
	kept := a.gaps[:0]
	for _, gap := range a.gaps {
		if gap.End.After(cutoff) {
			kept = append(kept, gap)
		}
	}
	a.gaps = kept
}

func (a *trafficAccounting) persistIfDue(now time.Time) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.dirty || a.path == "" || !a.lastPersist.IsZero() && now.Sub(a.lastPersist) < a.persistInterval {
		return nil
	}
	err := a.persistLocked()
	if err == nil {
		a.lastPersist = now
		a.dirty = false
	}
	return err
}

func (a *trafficAccounting) Flush() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.path == "" || !a.dirty {
		return nil
	}
	if err := a.persistLocked(); err != nil {
		return err
	}
	a.lastPersist = time.Now().UTC()
	a.dirty = false
	return nil
}

func (a *trafficAccounting) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(a.path), 0700); err != nil && filepath.Dir(a.path) != "." {
		return err
	}
	buckets := make([]trafficBucket, 0, len(a.buckets))
	for start, totals := range a.buckets {
		buckets = append(buckets, trafficBucket{Start: start, Totals: totals})
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Start.Before(buckets[j].Start) })
	state := persistedTrafficAccounting{Version: 1, Generation: a.generation, LastSampleAt: a.lastSampleAt, Totals: a.totals, Buckets: buckets, Gaps: append([]accountingGap(nil), a.gaps...)}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	temporary := a.path + ".tmp"
	if err := os.WriteFile(temporary, data, 0600); err != nil {
		return err
	}
	if err := os.Rename(temporary, a.path); err != nil {
		_ = os.Remove(a.path)
		if retry := os.Rename(temporary, a.path); retry != nil {
			_ = os.Remove(temporary)
			return retry
		}
	}
	return nil
}

func (a *trafficAccounting) CurrentSession(snapshot TelemetrySnapshot) volumeTotals {
	result := volumeTotals{}
	for _, session := range snapshot.Sessions {
		if !session.ClosedAt.IsZero() {
			continue
		}
		for leg := range session.Legs {
			row := session.Legs[leg]
			value := volumeTotals{}
			if leg == 0 {
				value.Leg0CarrierBytes = row.DataSentBytes
				value.Leg0UsefulBytes = row.LogicalTxAckedBytes
			} else {
				value.Leg1CarrierBytes = row.DataSentBytes
				value.Leg1UsefulBytes = row.LogicalTxAckedBytes
			}
			if row.IngressRole == TelemetryIngressNative {
				if leg == 0 {
					value.NativeLeg0CarrierBytes = row.DataSentBytes
					value.NativeLeg0UsefulBytes = row.LogicalTxAckedBytes
				} else {
					value.NativeLeg1CarrierBytes = row.DataSentBytes
					value.NativeLeg1UsefulBytes = row.LogicalTxAckedBytes
				}
			}
			if row.IngressRole == TelemetryIngressStandalone {
				if leg == 0 {
					value.StandaloneLeg0CarrierBytes = row.DataSentBytes
					value.StandaloneLeg0UsefulBytes = row.LogicalTxAckedBytes
				} else {
					value.StandaloneLeg1CarrierBytes = row.DataSentBytes
					value.StandaloneLeg1UsefulBytes = row.LogicalTxAckedBytes
				}
			}
			result = addVolumeTotals(result, value)
		}
	}
	return result
}

func (a *trafficAccounting) Report(period string, now time.Time, current TelemetrySnapshot) TrafficReport {
	if period == "" {
		period = "today"
	}
	period = strings.ToLower(period)
	result := TrafficReport{Period: period, Timezone: a.location.String(), Status: accountingUnknown, MetricSemantics: map[string]string{"carrier": "authoritative per-leg data_sent bytes; retransmission/duplicate attempts remain included", "useful": "authoritative logical_tx_acked_bytes (Useful ACK) counter; not renamed or reinterpreted"}}
	var totals volumeTotals
	var start time.Time
	switch period {
	case "current_session", "session":
		totals = a.CurrentSession(current)
		a.mu.RLock()
		haveSamples := !a.lastSampleAt.IsZero()
		a.mu.RUnlock()
		if haveSamples {
			result.Status = accountingComplete
		}
	case "today":
		local := now.In(a.location)
		start = time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, a.location).UTC()
	case "last_24_hours", "24h":
		start = now.Add(-24 * time.Hour)
	case "7_days", "7d":
		start = now.Add(-7 * 24 * time.Hour)
	case "all_recorded", "all":
		a.mu.RLock()
		totals = a.totals
		result.Gaps = append([]accountingGap(nil), a.gaps...)
		haveSamples := !a.lastSampleAt.IsZero()
		a.mu.RUnlock()
		if haveSamples {
			result.Status = accountingComplete
		} else {
			result.Status = accountingUnknown
		}
	default:
		result.Period = "today"
		local := now.In(a.location)
		start = time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, a.location).UTC()
	}
	if start != (time.Time{}) {
		end := now.UTC()
		a.mu.RLock()
		for bucketStart, bucket := range a.buckets {
			if !bucketStart.Before(end) || !bucketStart.Add(time.Minute).After(start) {
				continue
			}
			totals = addVolumeTotals(totals, bucket)
		}
		for _, gap := range a.gaps {
			if gap.End.After(start) && gap.Start.Before(end) {
				result.Gaps = append(result.Gaps, gap)
			}
		}
		haveSamples := !a.lastSampleAt.IsZero()
		a.mu.RUnlock()
		if haveSamples {
			result.Status = accountingComplete
		} else {
			result.Status = accountingUnknown
		}
	}
	for _, gap := range result.Gaps {
		if gap.End.After(start) || start.IsZero() {
			result.Status = accountingPartial
			result.LowerBound = true
			result.GapSeconds += int64(gap.End.Sub(gap.Start).Seconds())
		}
	}
	result.Volume = makeTrafficVolume(totals)
	return result
}

func makeTrafficVolume(t volumeTotals) TrafficVolume {
	result := TrafficVolume{Leg0CarrierBytes: t.Leg0CarrierBytes, Leg1CarrierBytes: t.Leg1CarrierBytes, Leg0UsefulBytes: t.Leg0UsefulBytes, Leg1UsefulBytes: t.Leg1UsefulBytes, NativeCarrierBytes: t.NativeLeg0CarrierBytes + t.NativeLeg1CarrierBytes, StandaloneCarrierBytes: t.StandaloneLeg0CarrierBytes + t.StandaloneLeg1CarrierBytes, NativeUsefulBytes: t.NativeLeg0UsefulBytes + t.NativeLeg1UsefulBytes, StandaloneUsefulBytes: t.StandaloneLeg0UsefulBytes + t.StandaloneLeg1UsefulBytes}
	result.CombinedCarrierBytes = result.Leg0CarrierBytes + result.Leg1CarrierBytes
	result.CombinedUsefulBytes = result.Leg0UsefulBytes + result.Leg1UsefulBytes
	result.Leg0CarrierDisplay, result.Leg1CarrierDisplay, result.CombinedCarrierDisplay = formatBytes(result.Leg0CarrierBytes), formatBytes(result.Leg1CarrierBytes), formatBytes(result.CombinedCarrierBytes)
	result.Leg0UsefulDisplay, result.Leg1UsefulDisplay, result.CombinedUsefulDisplay = formatBytes(result.Leg0UsefulBytes), formatBytes(result.Leg1UsefulBytes), formatBytes(result.CombinedUsefulBytes)
	result.NativeCarrierDisplay, result.StandaloneCarrierDisplay = formatBytes(result.NativeCarrierBytes), formatBytes(result.StandaloneCarrierBytes)
	result.NativeUsefulDisplay, result.StandaloneUsefulDisplay = formatBytes(result.NativeUsefulBytes), formatBytes(result.StandaloneUsefulBytes)
	if result.CombinedCarrierBytes > 0 {
		ratio := float64(result.CombinedUsefulBytes) / float64(result.CombinedCarrierBytes)
		result.UsefulCarrierRatio = &ratio
	}
	return result
}

func formatBytes(value uint64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	amount := float64(value)
	index := 0
	for amount >= 1024 && index < len(units)-1 {
		amount /= 1024
		index++
	}
	if index == 0 {
		return fmt.Sprintf("%d %s", value, units[index])
	}
	return fmt.Sprintf("%.2f %s", amount, units[index])
}

func (a *trafficAccounting) History(resolution string, now time.Time) TrafficHistoryResponse {
	if resolution != "day" && resolution != "hour" {
		resolution = "hour"
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	groups := make(map[time.Time]volumeTotals)
	for start, bucket := range a.buckets {
		local := start.In(a.location)
		var key time.Time
		if resolution == "day" {
			key = time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, a.location).UTC()
		} else {
			key = time.Date(local.Year(), local.Month(), local.Day(), local.Hour(), 0, 0, 0, a.location).UTC()
		}
		groups[key] = addVolumeTotals(groups[key], bucket)
	}
	items := make([]trafficBucket, 0, len(groups))
	for start, totals := range groups {
		items = append(items, trafficBucket{Start: start, Totals: totals})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Start.Before(items[j].Start) })
	status := accountingUnknown
	if !a.lastSampleAt.IsZero() {
		status = accountingComplete
	}
	if len(a.gaps) > 0 {
		status = accountingPartial
	}
	return TrafficHistoryResponse{Resolution: resolution, Timezone: a.location.String(), Status: status, Items: items}
}
