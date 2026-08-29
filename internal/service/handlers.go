package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/xdward/auction-contracts/gen/go"
	"github.com/xdward/auction/internal/auctionstore"
	"github.com/xdward/auction/util"
	"google.golang.org/protobuf/proto"
)

// SellHandler returns a function that processes a sell request and schedules its expiration.
func SellHandler(auction *auctionstore.Client, nc *nats.Conn) func(msg *nats.Msg) {
	stream, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}

	return func(msg *nats.Msg) {
		slog.Debug("received message")

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		// deserialize the protobuf request stored in the nats message
		var sellRequest pb.SellRequest
		if err := proto.Unmarshal(msg.Data, &sellRequest); err != nil {
			slog.Error("failed to deserialize sell request",
				slog.String("error", err.Error()),
				slog.Any("data", msg.Data),
			)
			msg.Respond([]byte("error"))
			return
		}

		// record the current time and calculate the expiration time
		start, end := util.DurationTimestamps(sellRequest.Duration)

		// perform the auction sell action
		success, err := auction.Sell(ctx, &sellRequest, start, end)
		if err != nil {
			slog.Error("failed to process sell request",
				slog.String("error", err.Error()),
				slog.Any("request", &sellRequest),
				slog.Time("start", start),
				slog.Time("end", end),
			)
			msg.Respond([]byte("error"))
			return
		}

		if success {
			// create a scheduled nats message for expiration
			scheduleMsg, err := newScheduleMessage(sellRequest.ItemId, end)
			if err != nil {
				slog.Error("failed to create schedule message",
					slog.String("error", err.Error()),
					slog.Any("request", &sellRequest),
					slog.Any("schedule", scheduleMsg),
				)
				msg.Respond([]byte("error"))
				return
			}

			// publish the schedule
			_, err = stream.PublishMsg(ctx, scheduleMsg)
			if err != nil {
				slog.Error("failed to publish schedule message",
					slog.String("error", err.Error()),
					slog.Any("request", &sellRequest),
					slog.Any("schedule", scheduleMsg),
				)
				msg.Respond([]byte("error"))
				return
			}
		}

		// reply to the nats request with the serialized protobuf response
		sellResponse := pb.SellResponse{
			Success: success,
		}
		replyMsg, err := proto.Marshal(&sellResponse)
		if err != nil {
			slog.Error("failed to serialize sell response",
				slog.String("error", err.Error()),
				slog.Any("request", &sellRequest),
				slog.Any("response", &sellResponse),
			)
			msg.Respond([]byte("error"))
			return
		}
		msg.Respond(replyMsg)

		slog.Debug("delivered response")
	}
}

// BidHandler returns a function that processes a bid request.
func BidHandler(auction *auctionstore.Client) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		slog.Debug("received message")

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		// deserialize the protobuf request stored in the nats message
		var bidRequest pb.BidRequest
		if err := proto.Unmarshal(msg.Data, &bidRequest); err != nil {
			slog.Error("failed to deserialize bid request",
				slog.String("error", err.Error()),
				slog.Any("data", msg.Data),
			)
			msg.Respond([]byte("error"))
			return
		}

		// perform the auction bid action
		success, err := auction.Bid(ctx, &bidRequest)
		if err != nil {
			slog.Error("failed to process bid request",
				slog.String("error", err.Error()),
				slog.Any("request", &bidRequest),
			)
			msg.Respond([]byte("error"))
			return
		}

		// reply to the nats request with the serialized protobuf response
		bidResponse := &pb.BidResponse{
			Success: success,
		}
		replyMsg, err := proto.Marshal(bidResponse)
		if err != nil {
			slog.Error("failed to serialize bid response",
				slog.String("error", err.Error()),
				slog.Any("request", &bidRequest),
				slog.Any("response", bidResponse),
			)
			msg.Respond([]byte("error"))
			return
		}
		msg.Respond(replyMsg)

		slog.Debug("delivered response")
	}
}

// CancelHandler returns a function that processes a cancel request.
func CancelHandler(auction *auctionstore.Client) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		slog.Debug("received message")

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		// deserialize the protobuf request stored in the nats message
		var cancelRequest pb.CancelRequest
		if err := proto.Unmarshal(msg.Data, &cancelRequest); err != nil {
			slog.Error("failed to deserialize cancel request",
				slog.String("error", err.Error()),
				slog.Any("data", msg.Data),
			)
			msg.Respond([]byte("error"))
			return
		}

		// perform the auction cancel action
		success, err := auction.Cancel(ctx, &cancelRequest)
		if err != nil {
			slog.Error("failed to process cancel request",
				slog.String("error", err.Error()),
				slog.Any("request", &cancelRequest),
			)
			msg.Respond([]byte("error"))
			return
		}

		// reply to the nats request with the serialized protobuf response
		cancelResponse := &pb.CancelResponse{
			Success: success,
		}
		replyMsg, err := proto.Marshal(cancelResponse)
		if err != nil {
			slog.Error("failed to serialize cancel response",
				slog.String("error", err.Error()),
				slog.Any("request", &cancelRequest),
				slog.Any("response", cancelResponse),
			)
			msg.Respond([]byte("error"))
			return
		}
		msg.Respond(replyMsg)

		slog.Debug("delivered response")
	}
}

// ExpireHandler returns a function that processes an expiration message for a listing.
func ExpireHandler(auction *auctionstore.Client) func(msg jetstream.Msg) {
	return func(msg jetstream.Msg) {
		slog.Debug("received message")

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		// decode the schedule data from the nats message
		var scheduleData scheduleMessageData
		if err := json.Unmarshal(msg.Data(), &scheduleData); err != nil {
			slog.Error("failed to deserialize scheduled message",
				slog.String("error", err.Error()),
				slog.Any("data", msg.Data()),
			)
			return
		}

		// perform the auction expire action
		err := auction.Expire(ctx, scheduleData.ItemID)
		if err != nil {
			// don't nak as it will either:
			// - instantly redeliver the message and spike cpu usage
			// - duplicate the original message; both will be redelivered
			slog.Warn("message returned, trying again later",
				slog.String("error", err.Error()),
				slog.Any("data", msg.Data()),
			)
			return
		}

		msg.Ack()

		slog.Debug("acknowledged message")
	}
}
