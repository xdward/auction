package db

import "errors"

var (
	AlreadyExistsErr = errors.New("item already exists")
	LowBidErr        = errors.New("the bid is lower")
	RedisScriptErr   = errors.New("unexpected script result")
)
