package messaging

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBuildScheduleMessage(t *testing.T) {
	expiration := time.Date(2027, time.December, 31, 23, 59, 0, 0, time.UTC)

	msg, err := BuildScheduleMessage(42, expiration)
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

	var payload ScheduleMessageData
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		t.Fatal(err.Error())
	}
	if payload.ItemID != 42 {
		t.Fatalf("unexpected payload: %d", payload)
	}
}

func TestScheduleStreamConfig(t *testing.T) {
	cfg, targetSubject := ScheduleStreamConfig()

	if cfg.Name != "AUCTION_SCHEDULES" {
		t.Fatalf("unexpected stream name: %q", cfg.Name)
	}

	if targetSubject != "auction.target.*" {
		t.Fatalf("unexpected target subject: %q", targetSubject)
	}

	if !cfg.AllowMsgSchedules {
		t.Fatal("expected schedules to be enabled")
	}

	if len(cfg.Subjects) != 2 ||
		cfg.Subjects[0] != "auction.schedule.*" ||
		cfg.Subjects[1] != "auction.target.*" {
		t.Fatalf("unexpected subjects: %#v", cfg.Subjects)
	}
}
