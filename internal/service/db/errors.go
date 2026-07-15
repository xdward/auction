package db

import "errors"

var (
	AlreadyExistsErr = errors.New("item already exists")
	RedisScriptErr   = errors.New("unexpected script result")
)
