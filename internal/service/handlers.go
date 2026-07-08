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

type WorkerResources struct {
	NATS *nats.Conn
	JS   jetstream.JetStream
}

type ScheduleMessageData struct {
	item_id uint64
}

func (wr *WorkerResources) HandleSell(msg *nats.Msg) {
	slog.Debug("received message")

	// deserialize protobuf request stored in the message
	var sellRequest pb.SellRequest
	if err := proto.Unmarshal(msg.Data, &sellRequest); err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}

	// TODO: update auction state

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// TODO: generate UUID
	uuid := "UUID"

	// create schedule message
	scheduleMsg := nats.NewMsg(fmt.Sprintf("expiration.schedule.%s", uuid))
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

	duration := time.Duration(sellRequest.Duration) * time.Millisecond
	expiration := time.Now().Add(duration).UTC()
	timestamp := expiration.Format(time.RFC3339)
	scheduleMsg.Header.Set("Nats-Schedule", fmt.Sprintf("@at %s", timestamp))
	scheduleMsg.Header.Set("Nats-Schedule-Target", fmt.Sprintf("expiration.target.%s", uuid))

	// publish schedule
	_, err = wr.JS.PublishMsg(ctx, scheduleMsg)
	if err != nil {
		slog.Error(err.Error())
		msg.Respond([]byte(err.Error()))
		return
	}

	// create protobuf response
	sellResponse := pb.SellResponse{
		Success: true,
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
func (wr *WorkerResources) HandleBuy(msg *nats.Msg) {
	slog.Debug("received message")
	msg.Respond([]byte("ok"))
	slog.Debug("delivered response")

}

// TODO
func (wr *WorkerResources) HandleCancel(msg *nats.Msg) {
	slog.Debug("received message")
	msg.Respond([]byte("ok"))
	slog.Debug("delivered response")

}

// TODO
func (wr *WorkerResources) HandleExpiration(msg jetstream.Msg) {
	slog.Debug("received message")
	msg.Ack()
	slog.Debug("ack")
}
