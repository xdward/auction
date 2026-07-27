package auctionstore

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// SnapshotScript:
// don't test the snapshot script directly, instead append new tests to snapshot_test.go

func TestUpdateScriptDirect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: server.Addr(),
	})
	defer rdb.Close()

	entry := []byte("payload")

	out, err := redis.NewScript(UpdateScript).Run(ctx, rdb, UpdateScriptKeys, entry).Result()
	if err != nil {
		t.Fatal(err)
	}

	result, ok := out.([]any)
	if !ok {
		t.Fatalf("unexpected result type %T", out)
	}

	if len(result) != 2 {
		t.Fatalf("unexpected result length: %d", len(result))
	}

	if result[0].(int64) != 1 {
		t.Fatalf("unexpected version: %d", result[0])
	}

	streamID, ok := result[1].(string)
	if !ok || streamID == "" {
		t.Fatalf("unexpected stream id: %q", result[1])
	}

	if result, err := rdb.Get(ctx, VersionKey).Result(); err != nil {
		t.Fatal(err.Error())
	} else if result != "1" {
		t.Fatalf("unexpected version key: %q", result)
	}

	if result, err := rdb.Get(ctx, VersionToEntryPrefix+"1").Result(); err != nil {
		t.Fatal(err.Error())
	} else if result != streamID {
		t.Fatalf("unexpected cursor mapping: %q", result)
	}

	if n, err := rdb.XLen(ctx, StreamKey).Result(); err != nil {
		t.Fatal(err.Error())
	} else if n != 1 {
		t.Fatalf("unexpected stream length: %d", n)
	}
}
