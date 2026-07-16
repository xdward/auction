package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/xdward/auction-contracts/gen/go"
	"google.golang.org/protobuf/proto"
)

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
	now := time.Now().UTC()
	duration := time.Duration(sellRequest.Duration) * time.Millisecond
	expiration := now.Add(duration)

	// perform the auction sell action
	success, err := w.DB.Sell(ctx, &sellRequest, now, expiration)
	if err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}

	// create a nats message and store the encoded schedule data
	scheduleMsg := nats.NewMsg(fmt.Sprintf("expire.schedule.%d", &sellRequest.ItemId))
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

	// set the schedule headers
	scheduleMsg.Header.Set(
		"Nats-Schedule",
		fmt.Sprintf("@at %s", expiration.Format(time.RFC3339)),
	)
	scheduleMsg.Header.Set(
		"Nats-Schedule-Target",
		fmt.Sprintf("expire.target.%d", &sellRequest.ItemId),
	)

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

// TODO
func (w *Worker) HandleExpire(msg jetstream.Msg) {
	slog.Debug("received message")
	msg.Ack()
	slog.Debug("ack")
}
