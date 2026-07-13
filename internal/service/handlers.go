package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"
	pb "github.com/xdward/auction-contracts/gen/go"
	"google.golang.org/protobuf/proto"
)

func (w *Worker) HandleSell(msg *nats.Msg) {
	slog.Debug("received message")

	// deserialize protobuf request stored in the message
	var sellRequest pb.SellRequest
	if err := proto.Unmarshal(msg.Data, &sellRequest); err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// capture current and expiration times
	now := time.Now().UTC()
	duration := time.Duration(sellRequest.Duration) * time.Millisecond
	expiration := now.Add(duration)

	// get key for the item
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
				CreatedAt: now.Unix(),
				ExpiresAt: expiration.Unix(),
				Active:    true,
			})
			// add a reference to the listing to the insertion order set
			pipe.ZAdd(ctx, ListingInsertion, redis.Z{
				Score:  float64(now.UnixMicro()),
				Member: key,
			})

			return nil
		})

		return err
	}

	// flag that indicates if the sell event was successful
	success := true

	// execute the transaction under the watch command
	if err := w.Redis.Watch(ctx, txf, key); err != nil {
		if err == AlreadyExistsErr {
			success = false
		} else {
			slog.Error(err.Error())
			msg.Respond([]byte(err.Error()))
			return
		}
	}

	// create schedule message
	scheduleMsg := nats.NewMsg(fmt.Sprintf("expiration.schedule.%d", &sellRequest.ItemId))
	scheduleData := ScheduleMessageData{
		item_id: sellRequest.ItemId,
	}
	payload, err := json.Marshal(scheduleData)
	if err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}
	scheduleMsg.Data = payload

	scheduleMsg.Header.Set(
		"Nats-Schedule",
		fmt.Sprintf("@at %s", expiration.Format(time.RFC3339)),
	)
	scheduleMsg.Header.Set(
		"Nats-Schedule-Target",
		fmt.Sprintf("expiration.target.%d", &sellRequest.ItemId),
	)

	// publish schedule
	_, err = w.JS.PublishMsg(ctx, scheduleMsg)
	if err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}

	// create protobuf response
	sellResponse := pb.SellResponse{
		Success: success,
	}

	// reply with the serialized protobuf response
	replyMsg, err := proto.Marshal(&sellResponse)
	if err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}
	msg.Respond(replyMsg)

	slog.Debug("delivered response")
}

// TODO
func (w *Worker) HandleBid(msg *nats.Msg) {
	slog.Debug("received message")
	msg.Respond([]byte("ok"))
	slog.Debug("delivered response")

}

// TODO
func (w *Worker) HandleCancel(msg *nats.Msg) {
	slog.Debug("received message")
	msg.Respond([]byte("ok"))
	slog.Debug("delivered response")

}

// TODO
func (w *Worker) HandleExpire(msg jetstream.Msg) {
	slog.Debug("received message")
	msg.Ack()
	slog.Debug("ack")
}
