package main

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/vearne/rmq"
	"time"
)

func main() {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "xxxx", // no password set
		DB:       0,      // use default DB
	})
	queueName := "testQueue"
	rmqClient := rmq.NewRMQClient(redisClient, queueName)

	for i := 0; i < 50; i++ {
		x, err := rmqClient.ReadMessage()
		if err != nil && err != redis.Nil {
			fmt.Println("msg:", x, "error", err)
		} else if err == redis.Nil {
			fmt.Println("There is no messages to read")
		} else {
			fmt.Println("message:", x)
		}
	}

	fmt.Println("---consume1---")
	rmqClient.ReAddUnAckMessage()

	fmt.Println("---ReAddUnAckMessage---")

	for i := 0; i < 120; i++ {
		//x, err := rmqClient.ReadMessage()
		x, err := rmqClient.BlockReadMessage(time.Second)
		if err != nil && err != redis.Nil {
			fmt.Println("msg:", x, "error", err)
		} else if err == redis.Nil {
			fmt.Println("There is no messages to read")
		} else {
			fmt.Println("message:", x)
			rmqClient.Ack(x)
		}
	}

	fmt.Println("---consume2---")

}
