package db

import "github.com/redis/go-redis/v9"

type Client struct {
	rdb *redis.Client
}

func NewClient(opts *redis.Options) *Client {
	rdb := redis.NewClient(opts)

	return &Client{
		rdb: rdb,
	}
}

func (c *Client) Close() error {
	return c.rdb.Close()
}
