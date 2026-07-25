package db

import "github.com/redis/go-redis/v9"

type Client struct {
	rdb *redis.Client
}

// NewClient creates a Redis-backed auction client.
func NewClient(opts *redis.Options) *Client {
	rdb := redis.NewClient(opts)

	return &Client{
		rdb: rdb,
	}
}

// Close releases the underlying Redis client.
func (c *Client) Close() error {
	return c.rdb.Close()
}
