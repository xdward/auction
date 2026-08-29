package server

import (
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// natsRequestReply marshals a protobuf request, sends it over NATS, and unmarshals the reply.
func natsRequestReply[Req proto.Message, Res proto.Message](
	nc *nats.Conn,
	subj string,
	req Req,
	res Res,
) error {
	// Marshal the protobuf request into the NATS payload.
	payload, err := proto.Marshal(req)
	if err != nil {
		slog.Error("failed to serialize request",
			slog.String("error", err.Error()),
			slog.String("event", subj),
			slog.Any("request", req),
		)
		return status.Error(codes.InvalidArgument, "malformed request")
	}

	// Send the request to NATS and wait for the reply.
	responseMsg, err := nc.Request(subj, payload, time.Second)
	if err != nil {
		slog.Error("failed to forward request to nats",
			slog.String("error", err.Error()),
			slog.String("event", subj),
			slog.Any("request", req),
		)
		return status.Error(codes.Unavailable, "service unavailable")
	}

	// Unmarshal the reply into the protobuf response.
	if err := proto.Unmarshal(responseMsg.Data, res); err != nil {
		slog.Error("failed to deserialize nats response",
			slog.String("error", err.Error()),
			slog.String("event", subj),
			slog.Any("request", req),
		)
		return status.Error(codes.Internal, "failed to handle message")
	}

	return nil
}
