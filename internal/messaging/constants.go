package messaging

const (
	SCHEDULE_STREAM         string = "AUCTION_SCHEDULES"
	SCHEDULE_CONSUMER       string = "schedule-watcher"
	HOLDING_SUBJECT         string = "auction.schedule.*"
	DELIVERY_SUBJECT        string = "auction.target.*"
	HOLDING_SUBJECT_FORMAT  string = "auction.schedule.%d"
	DELIVERY_SUBJECT_FORMAT string = "auction.target.%d"
)
