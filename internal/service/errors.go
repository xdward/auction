package service

import "errors"

var (
	AlreadyExistsErr = errors.New("item already exists")
	RedisScriptErr   = errors.New("unexpected script result")
	TypeCastingErr   = errors.New("failed type cast")
)
