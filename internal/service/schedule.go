package service

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	SCHEDULE_STREAM         string = "AUCTION_SCHEDULES"
	SCHEDULE_CONSUMER       string = "schedule-watcher"
	HOLDING_SUBJECT         string = "auction.schedule.*"
	DELIVERY_SUBJECT        string = "auction.target.*"
	HOLDING_SUBJECT_FORMAT  string = "auction.schedule.%d"
	DELIVERY_SUBJECT_FORMAT string = "auction.target.%d"
)

// scheduleMessageData is the payload stored in a scheduled auction message.
type scheduleMessageData struct {
	// ItemID identifies the listing associated with the scheduled message.
	ItemID uint64 `json:"itemID"`
}

// buildScheduleConfigs builds the stream and consumer configs used for scheduled messages.
func buildScheduleConfigs() (jetstream.StreamConfig, jetstream.ConsumerConfig) {
	streamConfig := jetstream.StreamConfig{
		Name:              SCHEDULE_STREAM,
		Subjects:          []string{HOLDING_SUBJECT, DELIVERY_SUBJECT},
		AllowMsgSchedules: true,
	}

	consumerConfig := jetstream.ConsumerConfig{
		Durable:       SCHEDULE_CONSUMER,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: DELIVERY_SUBJECT,
	}

	return streamConfig, consumerConfig
}

// newScheduleMessage creates the scheduled NATS message used to expire a listing.
func newScheduleMessage(itemID uint64, expiration time.Time) (*nats.Msg, error) {
	scheduleData := scheduleMessageData{
		ItemID: itemID,
	}
	payload, err := json.Marshal(scheduleData)
	if err != nil {
		return nil, err
	}

	scheduleMsg := nats.NewMsg(fmt.Sprintf(HOLDING_SUBJECT_FORMAT, itemID))
	scheduleMsg.Header.Set("Nats-Schedule", fmt.Sprintf("@at %s", expiration.Format(time.RFC3339)))
	scheduleMsg.Header.Set("Nats-Schedule-Target", fmt.Sprintf(DELIVERY_SUBJECT_FORMAT, itemID))
	scheduleMsg.Data = payload

	return scheduleMsg, nil
}
