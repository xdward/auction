package messaging

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// scheduleMessageData is the payload stored in a scheduled auction message.
type scheduleMessageData struct {
	ItemID uint64 `json:"itemID"`
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
