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

func TestGetSnapshotEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Set up an isolated Redis instance with no listings.
	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})
	defer client.Close()

	snapshot, err := GetSnapshot(ctx, client.rdb)
	if err != nil {
		t.Fatal(err)
	}

	// An empty database should produce an empty snapshot with version 0.
	if snapshot.Version != "0" {
		t.Fatalf("unexpected version: got %q want %q", snapshot.Version, "0")
	}
	if len(snapshot.Listings) != 0 {
		t.Fatalf("expected no listings, got %d", len(snapshot.Listings))
	}
}

func TestGetSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Set up an isolated Redis instance for the test.
	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})
	defer client.Close()

	// Create an active listing that should appear in the snapshot.
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

	// Add a bid so the snapshot includes a nonzero current bid.
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

	// Add an inactive listing to verify that snapshots filter it out.
	inactiveKey := ListingKey(2)
	if err := client.rdb.HSet(ctx, inactiveKey, Listing{
		Item:      2,
		Seller:    3,
		Bid:       0,
		Bidder:    0,
		CreatedAt: start.Format(time.RFC3339),
		ExpiresAt: end.Format(time.RFC3339),
		Active:    false,
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.rdb.ZAdd(ctx, SortedSetKey, redis.Z{
		Score:  float64(start.UnixMicro()),
		Member: inactiveKey,
	}).Err(); err != nil {
		t.Fatal(err)
	}

	// Set a version so the snapshot returns it unchanged.
	if err := client.rdb.Set(ctx, VersionKey, "7", 0).Err(); err != nil {
		t.Fatal(err)
	}

	snapshot, err := GetSnapshot(ctx, client.rdb)
	if err != nil {
		t.Fatal(err)
	}

	// The snapshot should return the stored version.
	if snapshot.Version != "7" {
		t.Fatalf("unexpected version: got %q want %q", snapshot.Version, "7")
	}

	// Only the active listing should appear in the snapshot.
	if len(snapshot.Listings) != 1 {
		t.Fatalf("expected one active listing, got %d", len(snapshot.Listings))
	}

	listing := snapshot.Listings[0]
	if listing.ItemId != sellRequest.ItemId {
		t.Fatalf("unexpected item id: got %d want %d", listing.ItemId, sellRequest.ItemId)
	}
	if listing.CurrentBid != bidRequest.Amount {
		t.Fatalf("unexpected bid: got %d want %d", listing.CurrentBid, bidRequest.Amount)
	}
	if listing.Expiration != end.Format(time.RFC3339) {
		t.Fatalf("unexpected expire: got %q want %q", listing.Expiration, end.Format(time.RFC3339))
	}
}
