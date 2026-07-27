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

func TestGetSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})
	defer client.Close()

	sellRequest := pb.SellRequest{
		ItemId:   1,
		SellerId: 1,
		Duration: uint64(time.Duration(time.Hour).Milliseconds()),
	}

	start, end := util.DurationTimestamps(sellRequest.Duration)
	if success, err := client.Sell(ctx, &sellRequest, start, end); err != nil {
		t.Fatal(err)
	} else if !success {
		t.Fatal("failed to sell an item")
	}

	bidRequest := pb.BidRequest{
		ItemId:   sellRequest.ItemId,
		BidderId: 2,
		Amount:   100,
	}

	if success, err := client.Bid(ctx, &bidRequest); err != nil {
		t.Fatal(err)
	} else if !success {
		t.Fatal("failed to place a bid")
	}

	snapshot, err := GetSnapshot(ctx, client.rdb)
	if err != nil {
		t.Fatal(err)
	}

	if snapshot.Version != "2" {
		t.Fatalf("unexpected snapshot version: %q", snapshot.Version)
	}

	if len(snapshot.Listings) != 1 {
		t.Fatalf("expected one active listing, found %d", len(snapshot.Listings))
	}

	listing := snapshot.Listings[0]
	if listing.ItemId != sellRequest.ItemId {
		t.Fatalf("unexpected item id: %d", listing.ItemId)
	}
	if listing.CurrentBid != bidRequest.Amount {
		t.Fatalf("unexpected bid amount: %d", listing.CurrentBid)
	}
	if listing.Expiration != end.Format(time.RFC3339) {
		t.Fatalf("unexpected expiration timestamp: %q", listing.Expiration)
	}
}

func TestGetSnapshotEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})
	defer client.Close()

	snapshot, err := GetSnapshot(ctx, client.rdb)
	if err != nil {
		t.Fatal(err.Error())
	}

	if snapshot.Version != "0" {
		t.Fatalf("unexpected snapshot version: %q", snapshot.Version)
	}

	if len(snapshot.Listings) != 0 {
		t.Fatalf("expected zero listings, found %d", len(snapshot.Listings))
	}
}

func TestGenSnapshotInactiveFilter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	client := NewClient(&redis.Options{
		Addr: server.Addr(),
	})
	defer client.Close()

	sellRequest := pb.SellRequest{
		ItemId:   1,
		SellerId: 1,
		Duration: uint64(time.Duration(time.Hour).Milliseconds()),
	}

	start, end := util.DurationTimestamps(sellRequest.Duration)
	if success, err := client.Sell(ctx, &sellRequest, start, end); err != nil {
		t.Fatal(err)
	} else if !success {
		t.Fatal("failed to sell an item")
	}

	cancelRequest := pb.CancelRequest{
		ItemId:   sellRequest.ItemId,
		SellerId: sellRequest.SellerId,
	}

	if success, err := client.Cancel(ctx, &cancelRequest); err != nil {
		t.Fatal(err)
	} else if !success {
		t.Fatal("failed to place a bid")
	}

	snapshot, err := GetSnapshot(ctx, client.rdb)
	if err != nil {
		t.Fatal(err)
	}

	if snapshot.Version != "2" {
		t.Fatalf("unexpected snapshot version: %q", snapshot.Version)
	}

	if len(snapshot.Listings) != 0 {
		t.Fatalf("expected zero listings, found %d", len(snapshot.Listings))
	}
}
