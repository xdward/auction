package util

import (
	"math"
	"testing"
	"time"
)

func TestDurationTimestamps(t *testing.T) {
	start, end := DurationTimestamps(1500)

	if start.Location() != time.UTC {
		t.Fatalf("expected start to be in UTC, got %v", start.Location())
	}

	if end.Location() != time.UTC {
		t.Fatalf("expected end to be in UTC, got %v", end.Location())
	}

	if !end.After(start) {
		t.Fatal("expected end to be after start")
	}

	diff := end.Sub(start)
	if diff < 1499*time.Millisecond || diff > 1501*time.Millisecond {
		t.Fatalf("unexpected duration: %v", diff)
	}
}

func TestDurationTimestampsClampsLargeDurations(t *testing.T) {
	start, end := DurationTimestamps(math.MaxUint64)

	if !end.After(start) {
		t.Fatal("expected clamped end to be after start")
	}

	diff := end.Sub(start)
	if diff < time.Duration(math.MaxInt64)-time.Second {
		t.Fatalf("expected clamp near max duration, got %v", diff)
	}
}
