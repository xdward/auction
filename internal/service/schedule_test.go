package service

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewScheduleMessage(t *testing.T) {
	expiration := time.Date(2027, time.December, 31, 23, 59, 0, 0, time.UTC)

	msg, err := newScheduleMessage(42, expiration)
	if err != nil {
		t.Fatal(err.Error())
	}

	if subj := msg.Subject; subj != "auction.schedule.42" {
		t.Fatalf("unexpected message subject: %q", subj)
	}

	if header := msg.Header.Get("Nats-Schedule"); header != "@at "+expiration.Format(time.RFC3339) {
		t.Fatalf("unexpected schedule header: %q", header)
	}

	if header := msg.Header.Get("Nats-Schedule-Target"); header != "auction.target.42" {
		t.Fatalf("unexpected target header: %q", header)
	}

	expectedPayload := scheduleMessageData{
		ItemID: 42,
	}

	var payload scheduleMessageData
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		t.Fatal(err.Error())
	}
	if payload != expectedPayload {
		t.Fatalf("unexpected payload: %v", payload)
	}
}
