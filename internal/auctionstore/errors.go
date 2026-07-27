package auctionstore

import "errors"

var (
	AlreadyExistsErr = errors.New("item already exists")
	InactiveErr      = errors.New("listing is inactive")
	LowBidErr        = errors.New("the bid is lower")
	RedisScriptErr   = errors.New("unexpected script result")
)
