package auctionstore

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
//
// New listings are added to two collections in Redis: a hash and a sorted set.
//
// The hash is used to store a record for each auction listing. Each record uses the item id as part
// of its key to ensure that a single item cannot have multiple listings. Each record holds pairs of
// fields and values to store information about each listing.
//
//	{
//		"auction:listings:7" -> { ... }
//		"auction:listings:8" -> { ... }
//		...
//	}
//
// The sorted set is used to sort listings by the sequence that they are created. When creating a
// new listing, the current Unix time is captured and assigned as the score for the new listing.
//
//	{
//		("auction:listings:7", 4000001),
//		("auction:listings:8", 4000035),
//		...
//	}
//
// While creating a listing, the Redis client is watched for other transactions that may be using
// the same item id. Additionally, the HSET and ZADD commands are pipelined together as a
// transaction. These mechanisms are used to avoid race conditions and make calls to these functions
// thread-safe.
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
		if exists, err := tx.Exists(ctx, key).Result(); err != nil {
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
//
// This function uses Redis watch and transaction mechanisms to ensure atomic changes and prevent
// race conditions when changes occur simultaneously.
//
// Example:
//
// If two bids arrive at the same time (A > B), and bid B is processed first, the transaction for
// bid A will be canceled. In the worker handler, bid A will not be requeued and it is expected that
// the user attempts the action again. Conversely, if bid A is processed first, bid B will be
// discarded for being lower than the current highest bid.
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
		// check if the listing exists
		if exists, err := tx.Exists(ctx, key).Result(); err != nil {
			return err
		} else if exists == 0 {
			return NotFoundErr
		}

		// check if the listing is active
		if active, err := tx.HGet(ctx, key, "active").Bool(); err != nil {
			return err
		} else if !active {
			return InactiveErr
		}

		// ensure the new bid is higher than the current bid
		currentBid, err := tx.HGet(ctx, key, "bid").Uint64()
		if err != nil {
			return err
		} else if !(bidRequest.Amount > currentBid) {
			return LowBidErr
		}

		// block self bids
		bidderID, err := tx.HGet(ctx, key, "bidder").Uint64()
		if err != nil {
			return err
		} else if bidRequest.BidderId == bidderID {
			return UnauthorizedErr
		}

		// transactional pipeline for executing multiple commands in a single read/write:
		// 	1) update the bid and bidder fields
		// 	2) run the update script
		// 		a) increment the version key
		// 		b) add a new stream entry with the serialized event data
		// 		c) store the stream index to read updates from
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, key, "bid", bidRequest.Amount, "bidder", bidRequest.BidderId)
			pipe.Eval(ctx, UpdateScript, updateScriptKeys, entry)
			return nil
		})

		return err
	}

	// watch the listing key and execute the transactional function
	if err := c.rdb.Watch(ctx, txf, key); err != nil {
		if err == LowBidErr || err == InactiveErr || err == UnauthorizedErr || err == NotFoundErr {
			return false, nil
		} else {
			return false, err
		}
	}

	return true, nil
}

// Cancel marks an active listing as inactive.
//
// The auction state in Redis is updated to flag the item as cancelled, as long as the user is
// authorized to make this transaction. In other words, the request must be the seller. Any relevant
// information is streamed.
//
// Redis transactions are used for consistency.
func (c *Client) Cancel(ctx context.Context, cancelRequest *pb.CancelRequest) (bool, error) {
	key := ListingKey(cancelRequest.ItemId) // listing key

	// transaction function that stops execution when a watched key is changed
	txf := func(tx *redis.Tx) error {
		// check if the listing exists
		if exists, err := tx.Exists(ctx, key).Result(); err != nil {
			return err
		} else if exists == 0 {
			return NotFoundErr
		}

		// check if the listing is active
		if active, err := tx.HGet(ctx, key, "active").Bool(); err != nil {
			return err
		} else if !active {
			return InactiveErr
		}

		// check if the user is authorized to cancel the bid
		if sellerID, err := tx.HGet(ctx, key, "seller").Uint64(); err != nil {
			return err
		} else if cancelRequest.SellerId != sellerID {
			return UnauthorizedErr
		}

		// get the current bid and bidder
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
			pipe.Eval(ctx, UpdateScript, updateScriptKeys, entry)
			return nil
		})

		return err
	}

	// watch the listing key and execute the transactional function
	if err := c.rdb.Watch(ctx, txf, key); err != nil {
		if err == InactiveErr || err == UnauthorizedErr || err == NotFoundErr {
			return false, nil
		} else {
			return false, err
		}
	}

	return true, nil
}

// Expire closes a listing after its scheduled expiration.
//
// The worker queue should invoke this function after receiving a scheduled message. It will either:
//
//   - Conclude the auction listing and stream relevant information, if it is active
//   - Cleanup the auction listing and stream nothing, if it is inactive
//
// Redis transactions are used for consistency.
func (c *Client) Expire(ctx context.Context, itemID uint64) error {
	key := ListingKey(itemID) // listing key

	// transaction function that stops execution when a watched key is changed
	txf := func(tx *redis.Tx) error {
		// check if the listing is exists
		if exists, err := tx.Exists(ctx, key).Result(); err == redis.Nil {
			return nil // already deleted; there is nothing to do
		} else if err != nil {
			return err
		} else if exists == 0 {
			return NotFoundErr
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
			pipe.Eval(ctx, UpdateScript, updateScriptKeys, entry)
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
