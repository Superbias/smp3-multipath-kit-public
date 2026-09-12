package server

import (
	"path/filepath"
	"testing"
	"time"
)

func accountingTestSnapshot(at time.Time, rows ...TelemetrySessionSnapshot) TelemetrySnapshot {
	return TelemetrySnapshot{Timestamp: at, Sessions: rows}
}

func accountingTestSession(id, role string, leg0Sent, leg0Useful, leg1Sent, leg1Useful uint64) TelemetrySessionSnapshot {
	return TelemetrySessionSnapshot{
		DisplaySessionID: id,
		IngressRole:      role,
		Legs: [2]TelemetryLegSnapshot{
			{State: "up", IngressRole: role, Generation: 1, DataSentBytes: leg0Sent, LogicalTxAckedBytes: leg0Useful},
			{State: "up", IngressRole: role, Generation: 1, DataSentBytes: leg1Sent, LogicalTxAckedBytes: leg1Useful},
		},
	}
}

func TestTrafficAccountingGoldenSequenceAndReset(t *testing.T) {
	a, err := newTrafficAccounting("", "UTC", "generation-1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)
	a.Collect(accountingTestSnapshot(t0, accountingTestSession("session-a", TelemetryIngressNative, 1000, 100, 2000, 200)))
	a.Collect(accountingTestSnapshot(t0.Add(time.Second), accountingTestSession("session-a", TelemetryIngressNative, 5000, 500, 3000, 300)))
	a.Collect(accountingTestSnapshot(t0.Add(2*time.Second), accountingTestSession("session-a", TelemetryIngressNative, 9000, 900, 7000, 700)))

	all := a.Report("all_recorded", t0.Add(3*time.Second), TelemetrySnapshot{})
	if all.Volume.Leg0CarrierBytes != 8000 || all.Volume.Leg1CarrierBytes != 5000 || all.Volume.CombinedCarrierBytes != 13000 {
		t.Fatalf("golden carrier totals=%+v", all.Volume)
	}
	if all.Volume.Leg0UsefulBytes != 800 || all.Volume.Leg1UsefulBytes != 500 || all.Volume.CombinedUsefulBytes != 1300 {
		t.Fatalf("golden useful totals=%+v", all.Volume)
	}

	// A counter decrease is a reset baseline, never a uint64 wrap or negative delta.
	a.Collect(accountingTestSnapshot(t0.Add(3*time.Second), accountingTestSession("session-a", TelemetryIngressNative, 500, 50, 7500, 750)))
	a.Collect(accountingTestSnapshot(t0.Add(4*time.Second), accountingTestSession("session-a", TelemetryIngressNative, 1500, 150, 8000, 800)))
	all = a.Report("all_recorded", t0.Add(5*time.Second), TelemetrySnapshot{})
	if all.Volume.Leg0CarrierBytes != 9000 || all.Volume.Leg1CarrierBytes != 6000 || all.Volume.CombinedCarrierBytes != 15000 {
		t.Fatalf("reset-safe carrier totals=%+v", all.Volume)
	}
	if all.Volume.Leg0UsefulBytes != 900 || all.Volume.Leg1UsefulBytes != 600 || all.Volume.CombinedUsefulBytes != 1500 {
		t.Fatalf("reset-safe useful totals=%+v", all.Volume)
	}
}

func TestTrafficAccountingRoleSessionGapAndHistory(t *testing.T) {
	a, err := newTrafficAccounting("", "UTC", "generation-1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	native := accountingTestSession("native", TelemetryIngressNative, 1000, 900, 2000, 1800)
	standalone := accountingTestSession("standalone", TelemetryIngressStandalone, 3000, 2700, 4000, 3600)
	a.Collect(accountingTestSnapshot(t0, native, standalone))
	native = accountingTestSession("native", TelemetryIngressNative, 2500, 2200, 3500, 3100)
	standalone = accountingTestSession("standalone", TelemetryIngressStandalone, 5000, 4500, 7000, 6300)
	a.Collect(accountingTestSnapshot(t0.Add(2*time.Second), native, standalone))

	report := a.Report("all_recorded", t0.Add(2*time.Second), TelemetrySnapshot{})
	if report.Volume.NativeCarrierBytes != 3000 || report.Volume.StandaloneCarrierBytes != 5000 {
		t.Fatalf("role carrier totals=%+v", report.Volume)
	}
	if report.Volume.NativeUsefulBytes != 2600 || report.Volume.StandaloneUsefulBytes != 4500 {
		t.Fatalf("role useful totals=%+v", report.Volume)
	}

	// Removing a session drops its baseline. A later replacement starts fresh.
	a.Collect(accountingTestSnapshot(t0.Add(3 * time.Second)))
	a.Collect(accountingTestSnapshot(t0.Add(4*time.Second), accountingTestSession("native", TelemetryIngressNative, 50, 40, 60, 50)))
	a.Collect(accountingTestSnapshot(t0.Add(5*time.Second), accountingTestSession("native", TelemetryIngressNative, 150, 140, 160, 150)))
	report = a.Report("all_recorded", t0.Add(5*time.Second), TelemetrySnapshot{})
	if report.Volume.NativeCarrierBytes != 3200 || report.Volume.StandaloneCarrierBytes != 5000 {
		t.Fatalf("replacement was double-counted: %+v", report.Volume)
	}

	// A long sample interval creates an accounting gap and therefore a lower bound.
	a.Collect(accountingTestSnapshot(t0.Add(8*time.Second), accountingTestSession("native", TelemetryIngressNative, 250, 240, 260, 250)))
	report = a.Report("today", t0.Add(8*time.Second), TelemetrySnapshot{})
	if report.Status != accountingPartial || !report.LowerBound || report.GapSeconds <= 0 {
		t.Fatalf("gap status=%+v", report)
	}
	history := a.History("hour", t0.Add(8*time.Second))
	if len(history.Items) == 0 || history.Status != accountingPartial {
		t.Fatalf("history=%+v", history)
	}
}

func TestTrafficAccountingGenerationRestartAndPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "traffic.json")
	t0 := time.Date(2026, 9, 12, 1, 0, 0, 0, time.UTC)
	a, err := newTrafficAccounting(path, "UTC", "generation-1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	a.Collect(accountingTestSnapshot(t0, accountingTestSession("session-a", TelemetryIngressStandalone, 1000, 900, 0, 0)))
	a.Collect(accountingTestSnapshot(t0.Add(time.Second), accountingTestSession("session-a", TelemetryIngressStandalone, 3000, 2700, 0, 0)))
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}

	restarted, err := newTrafficAccounting(path, "UTC", "generation-2", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// The first post-restart sample is a new baseline; it cannot replay old bytes.
	restarted.Collect(accountingTestSnapshot(t0.Add(10*time.Second), accountingTestSession("session-a", TelemetryIngressStandalone, 500, 0, 0, 0)))
	restarted.Collect(accountingTestSnapshot(t0.Add(11*time.Second), accountingTestSession("session-a", TelemetryIngressStandalone, 1500, 0, 0, 0)))
	restartedReport := restarted.Report("all_recorded", t0.Add(11*time.Second), TelemetrySnapshot{})
	if restartedReport.Volume.StandaloneCarrierBytes != 3000 {
		t.Fatalf("restart double-counted bytes=%+v", restartedReport.Volume)
	}
	if restartedReport.Status != accountingPartial || len(restartedReport.Gaps) == 0 {
		t.Fatalf("restart gap was not retained: %+v", restartedReport)
	}

	reloaded, err := newTrafficAccounting(path, "UTC", "generation-2", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	loaded := reloaded.Report("all_recorded", t0.Add(11*time.Second), TelemetrySnapshot{})
	if loaded.Volume.StandaloneCarrierBytes != 2000 {
		t.Fatalf("unexpected persisted source before second flush=%+v", loaded.Volume)
	}
}
