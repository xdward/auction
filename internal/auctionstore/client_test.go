package auctionstore

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

	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})

	request := pb.SellRequest{
		ItemId:   1,
		SellerId: 1,
		Duration: uint64(time.Duration(time.Hour).Milliseconds()),
	}

	start, end := util.DurationTimestamps(request.Duration)

	if success, err := client.Sell(ctx, &request, start, end); err != nil {
		t.Fatal(err.Error())
	} else if !success {
		t.Fatal("failed to sell an item")
	}

	key := ListingKey(request.ItemId)

	if exists, err := client.rdb.Exists(ctx, key).Result(); err != nil {
		t.Fatal(err.Error())
	} else if exists == 0 {
		t.Fatal("listing not found")
	}

	if _, err := client.rdb.ZScore(ctx, SortedSetKey, key).Result(); err == redis.Nil {
		t.Fatal("sorted set does not contain the listing key")
	} else if err != nil {
		t.Fatal(err.Error())
	}

	if n, err := client.rdb.XLen(ctx, StreamKey).Result(); err != nil {
		t.Fatal(err.Error())
	} else if n != 1 {
		t.Fatalf("expected one sell event, got %d stream entries", n)
	}
}

func TestSellDuplicate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})

	request := pb.SellRequest{
		ItemId:   1,
		SellerId: 1,
		Duration: uint64(time.Hour.Milliseconds()),
	}

	key := ListingKey(request.ItemId)

	client.rdb.HSet(ctx, key, Listing{
		Item:      1,
		Seller:    1,
		Bid:       0,
		Bidder:    0,
		CreatedAt: "",
		ExpiresAt: "",
	})

	start, end := util.DurationTimestamps(request.Duration)

	if success, err := client.Sell(ctx, &request, start, end); err != nil {
		t.Fatal(err.Error())
	} else if success {
		t.Fatal("expected duplicate sell to fail")
	}
}

func TestBid(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})

	request := pb.BidRequest{
		ItemId:   1,
		BidderId: 2,
		Amount:   100,
	}

	key := ListingKey(request.ItemId)

	client.rdb.HSet(ctx, key, Listing{
		Item:      1,
		Seller:    1,
		Bid:       0,
		Bidder:    0,
		CreatedAt: "",
		ExpiresAt: "",
	})

	if success, err := client.Bid(ctx, &request); err != nil {
		t.Fatal(err.Error())
	} else if !success {
		t.Fatal("failed to place a bid")
	}

	if bid, err := client.rdb.HGet(ctx, key, "bid").Uint64(); err != nil {
		t.Fatal(err.Error())
	} else if bid != request.Amount {
		t.Fatalf("unexpected bid: got %d instead of %d", bid, request.Amount)
	}

	if bidder, err := client.rdb.HGet(ctx, key, "bidder").Uint64(); err != nil {
		t.Fatal(err.Error())
	} else if bidder != request.BidderId {
		t.Fatalf("unexpected bidder: got %d instead of %d", bidder, request.BidderId)
	}

	if n, err := client.rdb.XLen(ctx, StreamKey).Result(); err != nil {
		t.Fatal(err.Error())
	} else if n != 1 {
		t.Fatalf("expected bid event, got %d stream entries", n)
	}
}

func TestBidLow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})

	request := pb.BidRequest{
		ItemId:   1,
		BidderId: 2,
		Amount:   100,
	}

	key := ListingKey(request.ItemId)

	client.rdb.HSet(ctx, key, Listing{
		Item:      1,
		Seller:    1,
		Bid:       40_000,
		Bidder:    3,
		CreatedAt: "",
		ExpiresAt: "",
	})

	if success, err := client.Bid(ctx, &request); err != nil {
		t.Fatal(err.Error())
	} else if success {
		t.Fatal("expected low bid to fail")
	}
}

func TestBidCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})

	request := pb.BidRequest{
		ItemId:   1,
		BidderId: 2,
		Amount:   100,
	}

	key := ListingKey(request.ItemId)

	client.rdb.HSet(ctx, key, Listing{
		Item:      1,
		Seller:    1,
		Bid:       0,
		Bidder:    0,
		CreatedAt: "",
		ExpiresAt: "",
	})

	if err := client.rdb.Del(ctx, key).Err(); err != nil {
		t.Fatal(err.Error())
	}

	if success, err := client.Bid(ctx, &request); err != nil {
		t.Fatal(err.Error())
	} else if success {
		t.Fatal("expected bid on deleted listing to fail")
	}
}

func TestBidMissing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})

	request := pb.BidRequest{
		ItemId:   1,
		BidderId: 2,
		Amount:   100,
	}

	if success, err := client.Bid(ctx, &request); err != nil {
		t.Fatal(err.Error())
	} else if success {
		t.Fatal("expected bid on nonexistent listing to fail")
	}
}

func TestCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})

	request := pb.CancelRequest{
		ItemId:   1,
		SellerId: 1,
	}

	key := ListingKey(request.ItemId)

	client.rdb.HSet(ctx, key, Listing{
		Item:      1,
		Seller:    1,
		Bid:       0,
		Bidder:    0,
		CreatedAt: "",
		ExpiresAt: "",
	})

	if success, err := client.Cancel(ctx, &request); err != nil {
		t.Fatal(err.Error())
	} else if !success {
		t.Fatal("failed to cancel a listing")
	}

	if exists, err := client.rdb.Exists(ctx, key).Result(); err != nil {
		t.Fatal(err.Error())
	} else if exists != 0 {
		t.Fatal("listing should be deleted after cancel")
	}

	if cancelled, err := client.rdb.SIsMember(ctx, CancelledSetKey, "1").Result(); err != nil {
		t.Fatal(err.Error())
	} else if !cancelled {
		t.Fatal("listing should be recorded in the cancelled set")
	}

	if n, err := client.rdb.XLen(ctx, StreamKey).Result(); err != nil {
		t.Fatal(err.Error())
	} else if n != 1 {
		t.Fatalf("expected cancel event, got %d stream entries", n)
	}
}

func TestCancelTwice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})

	request := pb.CancelRequest{
		ItemId:   1,
		SellerId: 1,
	}

	key := ListingKey(request.ItemId)

	client.rdb.HSet(ctx, key, Listing{
		Item:      1,
		Seller:    1,
		Bid:       0,
		Bidder:    0,
		CreatedAt: "",
		ExpiresAt: "",
	})

	if err := client.rdb.Del(ctx, key).Err(); err != nil {
		t.Fatal(err.Error())
	}

	if success, err := client.Cancel(ctx, &request); err != nil {
		t.Fatal(err.Error())
	} else if success {
		t.Fatal("expected cancel on an already cancelled listing to fail")
	}
}

func TestCancelMissing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})

	cancelRequest := pb.CancelRequest{
		ItemId:   1,
		SellerId: 1,
	}

	if success, err := client.Cancel(ctx, &cancelRequest); err != nil {
		t.Fatal(err.Error())
	} else if success {
		t.Fatal("expected cancel on nonexistent listing to fail")
	}
}

func TestCancelUnauthorized(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})

	request := pb.CancelRequest{
		ItemId:   1,
		SellerId: 2,
	}

	key := ListingKey(request.ItemId)

	client.rdb.HSet(ctx, key, Listing{
		Item:      1,
		Seller:    1,
		Bid:       0,
		Bidder:    0,
		CreatedAt: "",
		ExpiresAt: "",
	})

	if success, err := client.Cancel(ctx, &request); err != nil {
		t.Fatal(err.Error())
	} else if success {
		t.Fatal("expected unauthorized cancel to fail")
	}
}

func TestExpire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})

	key := ListingKey(1)

	client.rdb.HSet(ctx, key, Listing{
		Item:      1,
		Seller:    1,
		Bid:       0,
		Bidder:    0,
		CreatedAt: "",
		ExpiresAt: "",
	})

	if err := client.Expire(ctx, 1); err != nil {
		t.Fatal(err.Error())
	}

	if exists, err := client.rdb.Exists(ctx, key).Result(); err != nil {
		t.Fatal(err.Error())
	} else if exists != 0 {
		t.Fatal("listing should be deleted")
	}

	if _, err := client.rdb.ZScore(ctx, SortedSetKey, key).Result(); err != redis.Nil {
		t.Fatal("listing key should be removed from the sorted set")
	} else if err != nil && err != redis.Nil {
		t.Fatal(err.Error())
	}

	if n, err := client.rdb.XLen(ctx, StreamKey).Result(); err != nil {
		t.Fatal(err.Error())
	} else if n != 1 {
		t.Fatalf("expected expire, got %d stream entries", n)
	}
}

func TestExpireCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})

	key := ListingKey(1)

	client.rdb.HSet(ctx, key, Listing{
		Item:      1,
		Seller:    1,
		Bid:       0,
		Bidder:    0,
		CreatedAt: "",
		ExpiresAt: "",
	})

	if err := client.rdb.Del(ctx, key).Err(); err != nil {
		t.Fatal(err.Error())
	}

	if err := client.rdb.SAdd(ctx, CancelledSetKey, "1").Err(); err != nil {
		t.Fatal(err.Error())
	}

	if err := client.Expire(ctx, 1); err != nil {
		t.Error("expected expire to return nil for cancelled listings")
	}

	if exists, err := client.rdb.Exists(ctx, key).Result(); err != nil {
		t.Fatal(err.Error())
	} else if exists != 0 {
		t.Fatal("cancelled listing should be deleted on expire")
	}

	if _, err := client.rdb.ZScore(ctx, SortedSetKey, key).Result(); err != redis.Nil {
		t.Fatal("cancelled listing should be removed from the sorted set")
	} else if err != nil && err != redis.Nil {
		t.Fatal(err.Error())
	}

	if cancelled, err := client.rdb.SIsMember(ctx, CancelledSetKey, "1").Result(); err != nil {
		t.Fatal(err.Error())
	} else if cancelled {
		t.Fatal("cancelled listing should be pruned from the cancelled set")
	}

	if n, err := client.rdb.XLen(ctx, StreamKey).Result(); err != nil {
		t.Fatal(err.Error())
	} else if n != 0 {
		t.Fatalf("expected no expire event for cancelled listing, got %d stream entries", n)
	}
}

func TestExpireMissing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})

	if err := client.Expire(ctx, 1); err != NotFoundErr {
		t.Error("expected expire to return NotFoundErr for nonexistent listings")
	}
}
