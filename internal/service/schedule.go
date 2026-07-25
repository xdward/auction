package service

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type ScheduleMessageData struct {
	ItemID uint64 `json:"item_id"`
}

// BuildScheduleMessage creates the scheduled NATS message used to expire a listing.
func BuildScheduleMessage(itemID uint64, expiration time.Time) (*nats.Msg, error) {
	scheduleMsg := nats.NewMsg(fmt.Sprintf("auction.schedule.%d", itemID))
	scheduleData := ScheduleMessageData{
		ItemID: itemID,
	}
	payload, err := json.Marshal(scheduleData)
	if err != nil {
		return nil, err
	}
	scheduleMsg.Data = payload
	scheduleMsg.Header.Set("Nats-Schedule", fmt.Sprintf("@at %s", expiration.Format(time.RFC3339)))
	scheduleMsg.Header.Set("Nats-Schedule-Target", fmt.Sprintf("auction.target.%d", itemID))
	return scheduleMsg, nil
}

// ScheduleStreamConfig builds the stream config used for scheduled messages. The stream config and
// the inbox subject that receives expired messages is returned.
func ScheduleStreamConfig() (jetstream.StreamConfig, string) {
	scheduleSubject := "auction.schedule.*"
	targetSubject := "auction.target.*"

	return jetstream.StreamConfig{
		Name:              "AUCTION_SCHEDULES",
		Subjects:          []string{scheduleSubject, targetSubject},
		AllowMsgSchedules: true,
	}, targetSubject
}
