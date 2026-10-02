package smp3core

import (
	"sort"
	"time"
)

type rawPacerStats struct {
	bytes, sleepRequested, sleepActual, busyYields, immediate, deadlineLate int64
	serviceIterations, serviceTimerFires, serviceNoCredit                   int64
	medianBurst, p95Burst, maxBurst, medianGapNs, p95GapNs, maxGapNs        int64
	medianEffectiveBurst, p95EffectiveBurst, maxEffectiveBurst              int64
	medianCredit, p95Credit, maxCredit, catchupEvents, consecutiveServices  int64
	synchronizedEvents                                                      int64
	nearestDeltaMedianNs, nearestDeltaP95Ns                                 int64
	phaseWithin100us, phaseWithin250us, phaseWithin500us, phaseWithin1ms    float64
	timerNearestDeltaMedianNs, timerNearestDeltaP95Ns                       int64
	actualGapMedianNs, actualGapP95Ns, actualGapMaxNs                       int64
	latenessMedianNs, latenessP95Ns, latenessP99Ns, latenessMaxNs           int64
	cpuSecondsPerGiB                                                        float64
}

func percentileInt64(values []int64, p float64) int64 {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	i := int(float64(len(values)-1)*p + 0.5)
	return values[i]
}

func serializerDeliveryMetrics(l *benchmarkLink) (int64, int64, int64, int64, int64, int64, int64) {
	l.serviceMu.Lock()
	times := append([]time.Time(nil), l.actualDeliveryTimes...)
	late := append([]int64(nil), l.deliveryLatenessNs...)
	l.serviceMu.Unlock()
	gaps := make([]int64, 0, len(times))
	for i := 1; i < len(times); i++ {
		gaps = append(gaps, times[i].Sub(times[i-1]).Nanoseconds())
	}
	return percentileInt64(gaps, .5), percentileInt64(gaps, .95), percentileInt64(gaps, 1), percentileInt64(late, .5), percentileInt64(late, .95), percentileInt64(late, .99), percentileInt64(late, 1)
}

func serviceMetrics(l *benchmarkLink) rawPacerStats {
	l.serviceMu.Lock()
	events := append([]benchmarkServiceEvent(nil), l.serviceEvents...)
	l.serviceMu.Unlock()
	bursts, gaps, credits := make([]int64, 0, len(events)), make([]int64, 0, len(events)), make([]int64, 0, len(events))
	effective := make([]int64, 0, len(events))
	var currentBurst, previousBytes int64
	var stats rawPacerStats
	for i, e := range events {
		bursts = append(bursts, e.bytes)
		credits = append(credits, e.creditBefore)
		if e.gapNs > 0 {
			gaps = append(gaps, e.gapNs)
		}
		separated := i > 0 && e.gapNs*max(l.bps, 1) >= previousBytes*8*int64(time.Second)
		if separated {
			effective = append(effective, currentBurst)
			currentBurst = 0
		} else if i > 0 {
			stats.consecutiveServices++
		}
		currentBurst += e.bytes
		previousBytes = e.bytes
		if e.afterTimer && e.creditBefore > e.bytes {
			stats.catchupEvents++
		}
	}
	if currentBurst > 0 {
		effective = append(effective, currentBurst)
	}
	stats.medianBurst, stats.p95Burst, stats.maxBurst = percentileInt64(bursts, .5), percentileInt64(bursts, .95), percentileInt64(bursts, 1)
	stats.medianGapNs, stats.p95GapNs, stats.maxGapNs = percentileInt64(gaps, .5), percentileInt64(gaps, .95), percentileInt64(gaps, 1)
	stats.medianEffectiveBurst, stats.p95EffectiveBurst, stats.maxEffectiveBurst = percentileInt64(effective, .5), percentileInt64(effective, .95), percentileInt64(effective, 1)
	stats.medianCredit, stats.p95Credit, stats.maxCredit = percentileInt64(credits, .5), percentileInt64(credits, .95), percentileInt64(credits, 1)
	return stats
}
