package auctionstore

import "errors"

var (
	AlreadyExistsErr = errors.New("item already exists")
	InactiveErr      = errors.New("listing is inactive")
	LowBidErr        = errors.New("the bid is lower")
	RedisScriptErr   = errors.New("unexpected script result")
	NotFoundErr      = errors.New("listing does not exist")
	UnauthorizedErr  = errors.New("action unauthorized")
	StreamDataErr    = errors.New("stream entry missing data")
	StreamDecodeErr  = errors.New("invalid stream entry")
)
