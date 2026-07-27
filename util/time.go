package util

import (
	"math"
	"time"
)

const maxMS = uint64(math.MaxInt64) / uint64(time.Millisecond)

// DurationTimestamps returns (start, end), where start is the current time and end is the start
// time plus ms milliseconds. Both timestamps are in UTC.
func DurationTimestamps(ms uint64) (time.Time, time.Time) {
	start := time.Now().UTC()

	var duration time.Duration
	if ms > maxMS {
		duration = time.Duration(math.MaxInt64) // clamp
	} else {
		duration = time.Duration(ms) * time.Millisecond
	}

	end := start.Add(duration)

	return start, end
}
