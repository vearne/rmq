package rmq

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"time"
)

const UnAckPrefix = "UnAck:"
const BatchSize = 50

// 2 keys in redis
// 1. {queue}	list
// 2. UnAck:{queue}		hash
type RMQClient struct {
	redisClient redis.Cmdable
	queueName   string
	unAckKey    string
}

func NewRMQClient(redisClient redis.Cmdable, queueName string) *RMQClient {
	client := RMQClient{}
	client.redisClient = redisClient
	client.queueName = queueName
	client.unAckKey = UnAckPrefix + client.queueName
	return &client
}

// Messages May be consumed more than once
func (client *RMQClient) ReAddUnAckMessage() error {
	tmpMap, err := client.redisClient.HGetAll(context.Background(), client.unAckKey).Result()
	if err != nil {
		return err
	}

	msgList := make([]string, 0, len(tmpMap))
	for _, msg := range tmpMap {
		msgList = append(msgList, msg)
	}

	for i := 0; i*BatchSize < len(msgList); i++ {
		start := i * BatchSize
		end := Min((i+1)*BatchSize, len(msgList))
		_, err = client.redisClient.LPush(context.Background(), client.queueName, msgList[start:end]).Result()
		if err != nil {
			return err
		}
	}
	return nil
}

func (client *RMQClient) AddMessage(message string) error {
	_, err := client.redisClient.HSet(context.Background(), client.unAckKey, CalcuSHA1(message), message).Result()
	if err != nil {
		return err
	}
	_, err = client.redisClient.LPush(context.Background(), client.queueName, message).Result()
	if err != nil {
		return err
	}
	return err
}

func (client *RMQClient) ReadMessage() (string, error) {
	message, err := client.redisClient.RPop(context.Background(), client.queueName).Result()
	if err != nil {
		return "", err
	}
	return message, nil
}

func (client *RMQClient) BlockReadMessage(timeout time.Duration) (string, error) {
	msgList, err := client.redisClient.BRPop(context.Background(), timeout, client.queueName).Result()
	if err != nil {
		return "", err
	}
	if len(msgList) == 2 {
		return msgList[1], nil
	}
	return "", errors.New("unknow error")
}

func (client *RMQClient) Ack(message string) error {
	_, err := client.redisClient.HDel(context.Background(), client.unAckKey, CalcuSHA1(message)).Result()
	if err != nil {
		return err
	}
	return nil
}
