package infrastructure

import (
	"context"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	client *redis.Client
)

func loadRedis() {
	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := client.Ping(ctx).Err()
	if err != nil {
		log.Fatal("Unable to connect redis: ", err)
	}
}

func GetRedisClient() *redis.Client {
	return client
}
