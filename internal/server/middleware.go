package server

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// UnaryLoggingInterceptor logs gRPC unary requests and their responses.
func UnaryLoggingInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	slog.Debug("request received",
		slog.String("method", info.FullMethod),
		slog.Any("request", req),
	)

	res, err := handler(ctx, req)

	if err != nil {
		slog.Debug("failed to handle request",
			slog.String("error", err.Error()),
			slog.String("method", info.FullMethod),
			slog.Any("request", req),
		)
	} else {
		slog.Debug("response sent",
			slog.String("method", info.FullMethod),
			slog.Any("request", req),
			slog.Any("response", res),
		)
	}

	return res, err
}

// StreamLoggingInterceptor logs gRPC stream requests before and after the stream handler runs.
func StreamLoggingInterceptor(
	srv any,
	stream grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	p, _ := peer.FromContext(stream.Context())

	slog.Debug("stream opened",
		slog.String("method", info.FullMethod),
		slog.Any("peer", p),
	)

	err := handler(srv, stream)

	if err != nil {
		slog.Debug("stream error occured",
			slog.String("err", err.Error()),
			slog.String("method", info.FullMethod),
			slog.Any("peer", p),
		)
	} else {
		slog.Debug("stream closed",
			slog.String("method", info.FullMethod),
			slog.Any("peer", p),
		)
	}

	return err
}
