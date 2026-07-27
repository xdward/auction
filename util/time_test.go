package util

import (
	"math"
	"testing"
	"time"
)

func TestDurationTimestamps(t *testing.T) {
	start, end := DurationTimestamps(1500)

	if start.Location() != time.UTC {
		t.Fatalf("unexpected time zone for start: %q", start.Location())
	}

	if end.Location() != time.UTC {
		t.Fatalf("unexpected time zone for end: %q", end.Location())
	}

	if !end.After(start) {
		t.Fatal("expected start < end")
	}

	diff := end.Sub(start)
	if diff < 1499*time.Millisecond || diff > 1501*time.Millisecond {
		t.Fatalf("duration mismatch: %q", diff)
	}
}

func TestDurationTimestampsClampsLargeDurations(t *testing.T) {
	start, end := DurationTimestamps(math.MaxUint64)

	if !end.After(start) {
		t.Fatal("expected start < clamped end")
	}

	diff := end.Sub(start)
	if diff < time.Duration(math.MaxInt64)-time.Second {
		t.Fatalf("expected clamp near max duration")
	}
}
