package data

import (
	"block-crawling/internal/conf"
	"github.com/go-redis/redis"
)

var RedisClient *redis.Client

const CHAINNAME = "chain_name:"

// NewRedisClient new a redisClient.
func NewRedisClient(conf *conf.Data) *redis.Client {
	RedisClient = redis.NewClient(&redis.Options{
		Addr:     conf.Redis.Address,
		DB:       int(conf.Redis.Db),
		Password: conf.Redis.Password,
	})
	return RedisClient
}
