package db

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	pb "github.com/xdward/auction-contracts/gen/go"
	"google.golang.org/protobuf/proto"
)

func (c *Client) Sell(
	ctx context.Context,
	sellRequest *pb.SellRequest,
	start time.Time,
	end time.Time,
) (bool, error) {
	// serialize stream entry
	encodedEvent, err := proto.Marshal(&pb.SellEvent{
		ItemId:     sellRequest.ItemId,
		SellerId:   sellRequest.SellerId,
		Expiration: end.Format(time.RFC3339),
	})
	if err != nil {
		return false, err
	}

	// build key
	key := BuildListingKey(sellRequest.ItemId)

	// transaction function that ensures a listing doesn't exist before creating it
	txf := func(tx *redis.Tx) error {
		// check if the listing already exists
		exists, err := tx.Exists(ctx, key).Result()
		if err != nil {
			return err
		} else if exists == 1 {
			return AlreadyExistsErr
		}

		// execute write commands atomically
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			// store the listing as a redis hash
			pipe.HSet(ctx, key, Listing{
				Item:      sellRequest.ItemId,
				Seller:    sellRequest.SellerId,
				Bid:       0,
				Bidder:    0,
				CreatedAt: start.Format(time.RFC3339),
				ExpiresAt: end.Format(time.RFC3339),
				Active:    true,
			})
			// add a reference to the listing to the insertion order set
			pipe.ZAdd(ctx, SortedSetKey, redis.Z{
				Score:  float64(start.UnixMicro()),
				Member: key,
			})
			// atomically:
			// 1) update version
			// 2) append event to stream
			// 3) set version_to_id:<v> = streamID
			pipe.Eval(ctx, UpdateScript, []string{
				VersionKey,
				StreamKey,
				VersionToEntryPrefix,
			}, SellEvent, encodedEvent)

			return nil
		})

		return err
	}

	// execute the transaction under the watch command
	if err := c.rdb.Watch(ctx, txf, key); err != nil {
		if err == AlreadyExistsErr {
			return false, nil
		} else {
			return false, err
		}
	}

	return true, nil
}

func (c *Client) Bid(ctx context.Context, bidRequest *pb.BidRequest) (bool, error) {
	key := BuildListingKey(bidRequest.ItemId) // listing key

	// serialize event data; it will stored in the appended stream entry
	entry, err := proto.Marshal(&pb.BidEvent{
		ItemId:   bidRequest.ItemId,
		BidderId: bidRequest.BidderId,
		Amount:   bidRequest.Amount,
	})
	if err != nil {
		return false, err
	}

	// transaction function that stops execution when a watched key is changed
	txf := func(tx *redis.Tx) error {
		// retrieve current bid and ensure the new bid is higher
		currentBid, err := tx.HGet(ctx, key, "bid").Uint64()
		if err != nil {
			return err
		}
		if !(bidRequest.Amount > currentBid) {
			return LowBidErr
		}

		// transactional pipeline for executing multiple commands in a single read/write:
		// 	1) update the bid and bidder fields
		// 	2) update the version key and store the stream index to read updates from
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, key, Listing{
				Bid:    bidRequest.Amount,
				Bidder: bidRequest.BidderId,
			})
			pipe.Eval(ctx, UpdateScript, UpdateScriptKeys, BidEvent, entry)
			return nil
		})

		return err
	}

	// watch the listing key and execute the transactional function
	if err := c.rdb.Watch(ctx, txf, key); err != nil {
		if err == LowBidErr {
			return false, nil
		} else {
			return false, err
		}
	}

	return true, nil
}
