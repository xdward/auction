package db

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	pb "github.com/xdward/auction-contracts/gen/go"
	"github.com/xdward/auction/util"
)

func TestSell(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Set up a dummy Redis instance for the test.
	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})
	defer client.Close()

	// Create a valid sell request.
	request := pb.SellRequest{
		ItemId:   1,
		SellerId: 1,
		Duration: uint64(time.Duration(time.Hour).Milliseconds()),
	}

	// Create the listing and generate its expected key.
	start, end := util.DurationTimestamps(request.Duration)
	success, err := client.Sell(ctx, &request, start, end)
	if err != nil {
		t.Fatal(err)
	} else if !success {
		t.Fatal("failed to sell an item")
	}
	key := ListingKey(request.ItemId)

	// The listing hash should exist after a successful sell.
	exists, err := client.rdb.Exists(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	} else if exists == 0 {
		t.Fatal("item not found")
	}

	// The listing key should also be indexed in the recent-listings sorted set.
	_, err = client.rdb.ZScore(ctx, SortedSetKey, key).Result()
	if err == redis.Nil {
		t.Fatal("listing key was not found in the sorted set")
	} else if err != nil {
		t.Fatal(err)
	}

	// A sell event should have been appended to the event stream.
	n, err := client.rdb.XLen(ctx, StreamKey).Result()
	if err != nil {
		t.Fatal(err)
	} else if n < 1 {
		t.Fatal("sell event was not appended to the stream")
	}
}

func TestBid(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Set up a dummy Redis instance for the test.
	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})
	defer client.Close()

	// Create an active listing first, since bids require an existing item.
	sellRequest := pb.SellRequest{
		ItemId:   1,
		SellerId: 1,
		Duration: uint64(time.Duration(time.Hour).Milliseconds()),
	}

	// Set up a listing that can be bid on.
	start, end := util.DurationTimestamps(sellRequest.Duration)
	success, err := client.Sell(ctx, &sellRequest, start, end)
	if err != nil {
		t.Fatal(err)
	} else if !success {
		t.Fatal("failed to sell an item")
	}

	// Create a bid higher than the default current bid.
	bidRequest := pb.BidRequest{
		ItemId:   sellRequest.ItemId,
		BidderId: 2,
		Amount:   100,
	}

	// Place the bid and verify it is accepted.
	success, err = client.Bid(ctx, &bidRequest)
	if err != nil {
		t.Fatal(err)
	} else if !success {
		t.Fatal("failed to place a bid")
	}
	key := ListingKey(bidRequest.ItemId)

	// The listing should now reflect the new highest bid.
	bid, err := client.rdb.HGet(ctx, key, "bid").Uint64()
	if err != nil {
		t.Fatal(err)
	} else if bid != bidRequest.Amount {
		t.Fatalf("unexpected bid: got %d want %d", bid, bidRequest.Amount)
	}

	// The bidder field should track who placed the bid.
	bidder, err := client.rdb.HGet(ctx, key, "bidder").Uint64()
	if err != nil {
		t.Fatal(err)
	} else if bidder != bidRequest.BidderId {
		t.Fatalf("unexpected bidder: got %d want %d", bidder, bidRequest.BidderId)
	}

	// A bid event should have been appended after the initial sell event.
	if n, err := client.rdb.XLen(ctx, StreamKey).Result(); err != nil {
		t.Fatal(err)
	} else if n < 2 {
		t.Fatalf("expected bid event to be appended to the stream, got %d entries", n)
	}
}

func TestCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Set up a dummy Redis instance for the test.
	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})
	defer client.Close()

	// Create and initialize an active listing.
	sellRequest := pb.SellRequest{
		ItemId:   1,
		SellerId: 1,
		Duration: uint64(time.Duration(time.Hour).Milliseconds()),
	}
	start, end := util.DurationTimestamps(sellRequest.Duration)
	success, err := client.Sell(ctx, &sellRequest, start, end)
	if err != nil {
		t.Fatal(err)
	} else if !success {
		t.Fatal("failed to sell an item")
	}

	// Place a bid so the cancel event can capture the current bid state.
	bidRequest := pb.BidRequest{
		ItemId:   sellRequest.ItemId,
		BidderId: 2,
		Amount:   100,
	}
	success, err = client.Bid(ctx, &bidRequest)
	if err != nil {
		t.Fatal(err)
	} else if !success {
		t.Fatal("failed to place a bid")
	}

	// Cancel the listing and verify the operation succeeds.
	cancelRequest := pb.CancelRequest{
		ItemId:   sellRequest.ItemId,
		SellerId: sellRequest.SellerId,
	}
	success, err = client.Cancel(ctx, &cancelRequest)
	if err != nil {
		t.Fatal(err)
	} else if !success {
		t.Fatal("failed to cancel listing")
	}

	// Cancellation should mark the listing inactive, not delete it.
	key := ListingKey(cancelRequest.ItemId)
	active, err := client.rdb.HGet(ctx, key, "active").Bool()
	if err != nil {
		t.Fatal(err)
	} else if active {
		t.Fatal("listing should have been marked inactive")
	}

	// A cancel event should have been appended after the sell and bid events.
	if n, err := client.rdb.XLen(ctx, StreamKey).Result(); err != nil {
		t.Fatal(err)
	} else if n < 3 {
		t.Fatalf("expected cancel event to be appended to the stream, got %d entries", n)
	}
}

func TestExpire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Set up an isolated Redis instance for the test.
	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})
	defer client.Close()

	// Create and initialize an active listing.
	sellRequest := pb.SellRequest{
		ItemId:   1,
		SellerId: 1,
		Duration: uint64(time.Duration(time.Hour).Milliseconds()),
	}
	start, end := util.DurationTimestamps(sellRequest.Duration)
	success, err := client.Sell(ctx, &sellRequest, start, end)
	if err != nil {
		t.Fatal(err)
	} else if !success {
		t.Fatal("failed to sell an item")
	}

	// Place a bid so expiration can record a sold listing.
	bidRequest := pb.BidRequest{
		ItemId:   sellRequest.ItemId,
		BidderId: 2,
		Amount:   100,
	}
	success, err = client.Bid(ctx, &bidRequest)
	if err != nil {
		t.Fatal(err)
	} else if !success {
		t.Fatal("failed to place a bid")
	}

	// Expire the listing and ensure the operation succeeds.
	if err := client.Expire(ctx, sellRequest.ItemId); err != nil {
		t.Fatal(err)
	}

	// Expiration should remove the listing hash entirely.
	key := ListingKey(sellRequest.ItemId)
	exists, err := client.rdb.Exists(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	} else if exists != 0 {
		t.Fatal("listing should have been deleted")
	}

	// Expiration should also remove the listing from the recent-listings set.
	if _, err := client.rdb.ZScore(ctx, SortedSetKey, key).Result(); err != redis.Nil {
		if err != nil {
			t.Fatal(err)
		}
		t.Fatal("listing key should have been removed from the sorted set")
	}

	// An expire event should have been appended after the sell and bid events.
	if n, err := client.rdb.XLen(ctx, StreamKey).Result(); err != nil {
		t.Fatal(err)
	} else if n < 3 {
		t.Fatalf("expected expire event to be appended to the stream, got %d entries", n)
	}
}
