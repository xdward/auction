package auctionstore

import "errors"

var (
	ErrListingExists     = errors.New("item already exists")
	ErrBidTooLow         = errors.New("the bid is lower")
	ErrScriptResult      = errors.New("unexpected script result")
	ErrNotFound          = errors.New("listing does not exist")
	ErrUnauthorized      = errors.New("action unauthorized")
	ErrMissingStreamData = errors.New("stream entry missing data")
	ErrInvalidStreamData = errors.New("invalid stream entry")
)
