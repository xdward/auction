package util

import "time"

func DurationTimestamps(ms uint64) (time.Time, time.Time) {
	start := time.Now().UTC()
	duration := time.Duration(ms) * time.Millisecond
	end := start.Add(duration)

	return start, end
}
