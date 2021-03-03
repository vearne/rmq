package main

import (
	"github.com/go-redis/redis/v8"
	"github.com/vearne/rmq"
	"strconv"
)

func main() {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "xxxx", // no password set
		DB:       0,      // use default DB
	})
	queueName := "testQueue"
	rmqClient := rmq.NewRMQClient(redisClient, queueName)
	for i := 0; i < 105; i++ {
		rmqClient.AddMessage("message" + strconv.Itoa(i))
	}
}
