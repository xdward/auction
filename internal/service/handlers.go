package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/xdward/auction-contracts/gen/go"
	"github.com/xdward/auction/util"
	"google.golang.org/protobuf/proto"
)

// HandleSell processes a sell request and schedules its expiration.
func (w *Worker) HandleSell(msg *nats.Msg) {
	slog.Debug("received message")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// deserialize the protobuf request stored in the nats message
	var sellRequest pb.SellRequest
	if err := proto.Unmarshal(msg.Data, &sellRequest); err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}

	// record the current time and calculate the expiration time
	start, end := util.DurationTimestamps(sellRequest.Duration)

	// perform the auction sell action
	success, err := w.DB.Sell(ctx, &sellRequest, start, end)
	if err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}

	// create a scheduled nats message for expiration
	scheduleMsg, err := BuildScheduleMessage(sellRequest.ItemId, end)
	if err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}

	// publish the schedule
	_, err = w.JS.PublishMsg(ctx, scheduleMsg)
	if err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}

	// reply to the nats request with the serialized protobuf response
	sellResponse := pb.SellResponse{
		Success: success,
	}
	replyMsg, err := proto.Marshal(&sellResponse)
	if err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}
	msg.Respond(replyMsg)

	slog.Debug("delivered response")
}

// HandleBid processes a bid request.
func (w *Worker) HandleBid(msg *nats.Msg) {
	slog.Debug("received message")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// deserialize the protobuf request stored in the nats message
	var bidRequest pb.BidRequest
	if err := proto.Unmarshal(msg.Data, &bidRequest); err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}

	// perform the auction bid action
	success, err := w.DB.Bid(ctx, &bidRequest)
	if err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}

	// reply to the nats request with the serialized protobuf response
	bidResponse := &pb.BidResponse{
		Success: success,
	}
	replyMsg, err := proto.Marshal(bidResponse)
	if err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}
	msg.Respond(replyMsg)

	slog.Debug("delivered response")
}

// HandleCancel processes a cancel request.
func (w *Worker) HandleCancel(msg *nats.Msg) {
	slog.Debug("received message")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// deserialize the protobuf request stored in the nats message
	var cancelRequest pb.CancelRequest
	if err := proto.Unmarshal(msg.Data, &cancelRequest); err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}

	// perform the auction cancel action
	success, err := w.DB.Cancel(ctx, &cancelRequest)
	if err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}

	// reply to the nats request with the serialized protobuf response
	cancelResponse := &pb.CancelResponse{
		Success: success,
	}
	replyMsg, err := proto.Marshal(cancelResponse)
	if err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}
	msg.Respond(replyMsg)

	slog.Debug("delivered response")
}

// HandleExpire processes an expiration message for a listing.
func (w *Worker) HandleExpire(msg jetstream.Msg) {
	slog.Debug("received message")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// decode the schedule data from the nats message
	var scheduleData ScheduleMessageData
	if err := json.Unmarshal(msg.Data(), &scheduleData); err != nil {
		slog.Error(err.Error())
		slog.Warn("message returned, trying again later")
		return
	}

	// perform the auction expire action
	err := w.DB.Expire(ctx, scheduleData.ItemID)
	if err != nil {
		slog.Error(err.Error())
		// don't nak as it will either:
		// - instantly redeliver the message and spike cpu usage
		// - duplicate the original message; both will be redelivered
		slog.Warn("message returned, trying again later")
		return
	}

	msg.Ack()
	slog.Debug("ack")
}
