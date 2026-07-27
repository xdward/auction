package db

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	pb "github.com/xdward/auction-contracts/gen/go"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	rdb *redis.Client
}

// NewClient creates a Redis-backed auction client.
func NewClient(opts *redis.Options) *Client {
	rdb := redis.NewClient(opts)

	return &Client{
		rdb: rdb,
	}
}

// Close releases the underlying Redis client.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Sell creates a new listing if it does not already exist.
func (c *Client) Sell(
	ctx context.Context,
	sellRequest *pb.SellRequest,
	start time.Time,
	end time.Time,
) (bool, error) {
	key := ListingKey(sellRequest.ItemId) // listing key

	// serialize event data; it will be stored in the appended stream entry
	encodedEvent, err := proto.Marshal(&pb.EventStreamResponse{
		Event: &pb.EventStreamResponse_SellEvent{
			SellEvent: &pb.SellEvent{
				ItemId:     sellRequest.ItemId,
				SellerId:   sellRequest.SellerId,
				Expiration: end.Format(time.RFC3339),
			},
		},
	})
	if err != nil {
		return false, err
	}

	// transaction function that stops execution when a watched key is changed
	txf := func(tx *redis.Tx) error {
		// ensure the listing doesn't exist
		exists, err := tx.Exists(ctx, key).Result()
		if err != nil {
			return err
		} else if exists > 0 {
			return AlreadyExistsErr
		}

		// transactional pipeline for executing multiple commands in a single read/write:
		// 	1) create a hash with the listing key, and store the listing record
		// 	2) add the listing key to the sorted set, with the start time as the score
		// 	3) run the update script
		// 		a) increment the version key
		// 		b) add a new stream entry with the serialized event data
		// 		c) store the stream index to read updates from
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, key, Listing{
				Item:      sellRequest.ItemId,
				Seller:    sellRequest.SellerId,
				Bid:       0,
				Bidder:    0,
				CreatedAt: start.Format(time.RFC3339),
				ExpiresAt: end.Format(time.RFC3339),
				Active:    true,
			})
			pipe.ZAdd(ctx, SortedSetKey, redis.Z{
				Score:  float64(start.UnixMicro()),
				Member: key,
			})
			pipe.Eval(ctx, UpdateScript, []string{
				VersionKey,
				StreamKey,
				VersionToEntryPrefix,
			}, encodedEvent)
			return nil
		})

		return err
	}

	// watch the listing key and execute the transactional function
	if err := c.rdb.Watch(ctx, txf, key); err != nil {
		if err == AlreadyExistsErr {
			return false, nil
		} else {
			return false, err
		}
	}

	return true, nil
}

// Bid updates a listing with a higher bid.
func (c *Client) Bid(ctx context.Context, bidRequest *pb.BidRequest) (bool, error) {
	key := ListingKey(bidRequest.ItemId) // listing key

	// serialize event data; it will be stored in the appended stream entry
	entry, err := proto.Marshal(&pb.EventStreamResponse{
		Event: &pb.EventStreamResponse_BidEvent{
			BidEvent: &pb.BidEvent{
				ItemId:   bidRequest.ItemId,
				BidderId: bidRequest.BidderId,
				Amount:   bidRequest.Amount,
			},
		},
	})
	if err != nil {
		return false, err
	}

	// transaction function that stops execution when a watched key is changed
	txf := func(tx *redis.Tx) error {
		// ensure the new bid is higher than the current bid
		currentBid, err := tx.HGet(ctx, key, "bid").Uint64()
		if err != nil {
			return err
		}
		if !(bidRequest.Amount > currentBid) {
			return LowBidErr
		}

		// transactional pipeline for executing multiple commands in a single read/write:
		// 	1) update the bid and bidder fields
		// 	2) run the update script
		// 		a) increment the version key
		// 		b) add a new stream entry with the serialized event data
		// 		c) store the stream index to read updates from
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, key, "bid", bidRequest.Amount, "bidder", bidRequest.BidderId)
			pipe.Eval(ctx, UpdateScript, UpdateScriptKeys, entry)
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

// Cancel marks an active listing as inactive.
func (c *Client) Cancel(ctx context.Context, cancelRequest *pb.CancelRequest) (bool, error) {
	key := ListingKey(cancelRequest.ItemId) // listing key

	// transaction function that stops execution when a watched key is changed
	txf := func(tx *redis.Tx) error {
		// check if the listing is active
		active, err := tx.HGet(ctx, key, "active").Bool()
		if err != nil {
			return err
		} else if !active {
			return InactiveErr
		}

		// get the current bid and the user holding it
		bid, err := tx.HGet(ctx, key, "bid").Uint64()
		if err != nil {
			return err
		}
		bidderID, err := tx.HGet(ctx, key, "bidder").Uint64()
		if err != nil {
			return err
		}

		// serialize event data; it will be stored in the appended stream entry
		entry, err := proto.Marshal(&pb.EventStreamResponse{
			Event: &pb.EventStreamResponse_CancelEvent{
				CancelEvent: &pb.CancelEvent{
					ItemId:   cancelRequest.ItemId,
					SellerId: cancelRequest.SellerId,
					BidderId: bidderID,
					Amount:   bid,
				},
			},
		})
		if err != nil {
			return err
		}

		// transactional pipeline for executing multiple commands in a single read/write:
		// 	1) update the bid and bidder fields
		// 	2) run the update script
		// 		a) increment the version key
		// 		b) add a new stream entry with the serialized event data
		// 		c) store the stream index to read updates from
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, key, "active", false)
			pipe.Eval(ctx, UpdateScript, UpdateScriptKeys, entry)
			return nil
		})

		return err
	}

	// watch the listing key and execute the transactional function
	if err := c.rdb.Watch(ctx, txf, key); err != nil {
		if err == InactiveErr {
			return false, nil
		} else {
			return false, err
		}
	}

	return true, nil
}

// Expire closes a listing after its scheduled expiration.
func (c *Client) Expire(ctx context.Context, itemID uint64) error {
	key := ListingKey(itemID) // listing key

	// transaction function that stops execution when a watched key is changed
	txf := func(tx *redis.Tx) error {
		// check if the listing is active
		exists, err := tx.Exists(ctx, key).Result()
		if err != nil {
			return nil // already deleted or doesn't exist; there is nothing to do
		} else if exists == 0 {
			return err
		}

		// get the current bid, the highest bidder, and the seller
		bid, err := tx.HGet(ctx, key, "bid").Uint64()
		if err != nil {
			return err
		}
		bidderID, err := tx.HGet(ctx, key, "bidder").Uint64()
		if err != nil {
			return err
		}
		sellerID, err := tx.HGet(ctx, key, "seller").Uint64()
		if err != nil {
			return err
		}

		// serialize event data; it will be stored in the appended stream entry
		entry, err := proto.Marshal(&pb.EventStreamResponse{
			Event: &pb.EventStreamResponse_ExpireEvent{
				ExpireEvent: &pb.ExpireEvent{
					ItemId:   itemID,
					Sold:     bid > 0,
					SellerId: sellerID,
					BidderId: bidderID,
					Amount:   bid,
				},
			},
		})
		if err != nil {
			return err
		}

		// transactional pipeline for executing multiple commands in a single read/write:
		// 	1) update the bid and bidder fields
		// 	2) run the update script
		// 		a) increment the version key
		// 		b) add a new stream entry with the serialized event data
		// 		c) store the stream index to read updates from
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, key)
			pipe.ZRem(ctx, SortedSetKey, key)
			pipe.Eval(ctx, UpdateScript, UpdateScriptKeys, entry)
			return nil
		})

		return err
	}

	// watch the listing key and execute the transactional function
	if err := c.rdb.Watch(ctx, txf, key); err != nil {
		return err
	}

	return nil
}
