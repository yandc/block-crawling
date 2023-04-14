package data

import (
	"block-crawling/internal/conf"
	"block-crawling/internal/log"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/go-redis/redis"
	"go.uber.org/zap"
)

var RedisClient *redis.Client

const CHAINNAME = "chain_name:"
const PENDINGTX = "chain_name:pending_txids:"

// NewRedisClient new a redisClient.
func NewRedisClient(conf *conf.Data) *redis.Client {
	RedisClient = redis.NewClient(&redis.Options{
		Addr:     conf.Redis.Address,
		DB:       int(conf.Redis.Db),
		Password: conf.Redis.Password,
	})
	RedisQueueManager = NewQueueManager(RedisClient)
	return RedisClient
}

var RedisQueueManager *QueueManager

type MessageHandler interface {
	Execute(*QueueReceiveMessage) *HandleResult
}

// Handler 返回值代表消息是否消费成功
type MessageHandlerFunc func(msg *QueueReceiveMessage) *HandleResult

// 发送消息
type QueueSendMessage struct {
	Topic     string      `json:"topic"`
	Partition string      `json:"group"`
	Body      interface{} `json:"body"`
}

// 接收消息
type QueueReceiveMessage struct {
	Topic     string // 消息的主题
	Partition string // 消息的分区
	Body      string // 消息的Body
}

// 消息处理结果
type HandleResult struct {
	State   bool        `json:"state"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

func NewQueueResult(state bool, msg string, data interface{}) *HandleResult {
	return &HandleResult{State: state, Message: msg, Data: data}
}

// 队列管理器
type QueueManager struct {
	client      *redis.Client
	MaxRetry    int
	RecoverChan chan RecoverData
	Handlers    map[string]interface{}
}

// 队列恢复的信息
type RecoverData struct {
	Topic     string
	Partition string
	Handler   interface{}
}

// 初始化队列管理器
func NewQueueManager(client *redis.Client) *QueueManager {
	redisQueueManager := &QueueManager{}
	redisQueueManager.client = client
	redisQueueManager.MaxRetry = 3
	redisQueueManager.Handlers = make(map[string]interface{})
	return redisQueueManager
}

func (r *QueueManager) SetRecoverChan(ch chan RecoverData) {
	r.RecoverChan = ch
}

func (r *QueueManager) GetRecoverChan() chan RecoverData {
	return r.RecoverChan
}

func (r *QueueManager) GetQueueName(topic string, partition string) string {
	var name string
	if len(partition) > 0 {
		name = fmt.Sprintf("%s:%s", topic, partition)
	} else {
		name = fmt.Sprintf("%s", topic)
	}
	return name
}

// 注册handler
func (r *QueueManager) RegisterHandler(topic string, partition string, handler interface{}) error {
	name := r.GetQueueName(topic, partition)
	if _, ok := r.Handlers[name]; ok {
		return errors.New("is exits")
	} else {
		r.Handlers[name] = handler
		go r.QueueConsume(topic, partition)
	}
	return nil
}

// 重启队列
func (r *QueueManager) RecoverQueue(recoverData RecoverData) {
	name := r.GetQueueName(recoverData.Topic, recoverData.Partition)
	if _, ok := r.Handlers[name]; ok {
		go r.QueueConsume(recoverData.Topic, recoverData.Partition)
	}
}

// 生产者执行入队列
func (r *QueueManager) QueuePublish(payload *QueueSendMessage) error {
	if len(payload.Topic) <= 0 {
		return errors.New("TopicId can not be empty")
	}
	name := r.GetQueueName(payload.Topic, payload.Partition)
	err := r.client.LPush(name, payload.Body).Err()
	return err
}

// 消费者执行出队列
func (r *QueueManager) QueueConsume(topic string, partition string) {
	defer func() {
		if err := recover(); err != nil {
			var stacktrace string
			for i := 1; ; i++ {
				_, f, l, got := runtime.Caller(i)
				if !got {
					break
				}
				stacktrace += fmt.Sprintf("%s:%d\n", f, l)
			}
			// when stack finishes
			logMessage := fmt.Sprintf("Trace: %s\n", err)
			logMessage += fmt.Sprintf("\n%s", stacktrace)
			log.Error(logMessage)

			if e, ok := err.(error); ok {
				log.Error("GetTransactions error", zap.Any("topic", topic), zap.Any("partition", partition), zap.Any("error", e))
			} else {
				log.Error("GetTransactions panic", zap.Any("topic", topic), zap.Any("partition", partition), zap.Any("error", errors.New(fmt.Sprintf("%s", err))))
			}

			//执行恢复函数
			r.handleRecover(topic, partition)
		}
	}()

	for {
		//消费者执行出列
		name := r.GetQueueName(topic, partition)
		result, err := r.client.BRPop(0, name).Result()
		if err != nil {
			return
		}
		if len(result) > 0 {
			//topicPartition := result[0]
			vals := result[1]
			payload := QueueReceiveMessage{
				Topic:     topic,
				Partition: partition,
				Body:      vals,
			}
			//执行回调函数
			r.handleCallBack(&payload)
		}
	}
}

// 消费者执行出队列
func (r *QueueManager) QueueGet(topic string, partition string, timeout time.Duration) (string, error) {
	name := r.GetQueueName(topic, partition)
	result, err := r.client.BRPop(timeout, name).Result()
	if err != nil {
		return "", err
	}
	if len(result) > 0 {
		//topicPartition := result[0]
		vals := result[1]
		return vals, nil
	}
	return "", nil
}

// 执行清空队列
func (r *QueueManager) QueueDel(topic string, partition string) (bool, error) {
	name := r.GetQueueName(topic, partition)
	result, err := r.client.Del(name).Result()
	if err != nil {
		return false, err
	}
	return result > 0, nil
}

// 执行恢复函数
func (r *QueueManager) handleRecover(topic string, partition string) {
	handlerName := r.GetQueueName(topic, partition)
	handler, ok := r.Handlers[handlerName]
	if r.RecoverChan != nil && ok {
		r.RecoverChan <- RecoverData{topic, partition, handler}
	}
}

// 执行回调函数
func (r *QueueManager) handleCallBack(payload *QueueReceiveMessage) {
	handlerName := r.GetQueueName(payload.Topic, payload.Partition)
	handler := r.Handlers[handlerName]
	if handler != nil {
		if ob, ok := handler.(MessageHandler); ok {
			//同步执行Max次，保证队列顺序，失败则丢弃消息,
			for i := 0; i < r.MaxRetry; i++ {
				rs := ob.Execute(payload)
				if rs.State {
					break
				}
			}
		} else if ob, ok := handler.(func(msg *QueueReceiveMessage) *HandleResult); ok {
			//同步执行Max次，保证队列顺序，失败则丢弃消息,
			for i := 0; i < r.MaxRetry; i++ {
				rs := ob(payload)
				if rs.State {
					break
				}
			}
		} else {
			log.Error("no message Handler", zap.Any("handler", handler), zap.Any("payload", payload))
		}
	}
}
