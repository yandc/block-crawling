package biz

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

func PushTopicCMQ(chainName, topicId, msg, topicURL string) error {
	eventId := uuid.NewV1().String()
	log.Info("PushTopicCMQ info:", zap.Any("eventId", eventId))
	var out *eventOut
	err := httpclient.HttpPostJson(
		topicURL,
		map[string]interface{}{
			"eventId": eventId,
			"topicId": topicId,
			"msg":     msg,
		},
		&out, nil,
	)
	log.Info("PushTopicCMQ result:", zap.Any("result", out))
	if err != nil {
		// redis出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链推送ParyCard信息，推送ParyCard信息到 CMQ 中失败", chainName)
		alarmOpts := WithMsgLevel("FATAL")
		alarmOpts = WithAlarmChannel("node-proxy")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("推送ParyCard信息，推送ParyCard信息到 CMQ 中失败", zap.Any("chainName", chainName), zap.Any("error", err), zap.Any("msg", msg))
		return err
	}
	return nil
}

func RabbitMQPush(addr,queueName,msg string){
	log.Info("RabbitMQPush start",zap.Any("addr",addr))
	// 连接到RabbitMQ服务器
	conn, err := amqp.Dial("amqp://"+addr)
	if err != nil {
		log.Error("RabbitMQPush Failed to connect to RabbitMQ",zap.Error(err))
		return
	}

	defer conn.Close()

	// 创建一个channel
	ch, err := conn.Channel()
	if err != nil {
		log.Error("RabbitMQPush Failed to open a channel",zap.Error(err))
		return
	}

	defer ch.Close()


	// 声明一个队列
	q, err := ch.QueueDeclare(
		queueName, // 队列名称
		true,   // 是否持久化
		false,   // 是否自动删除
		false,   // 是否排他
		false,   // 是否随机名称
		nil,     // 额外属性
	)
	if err != nil {
		log.Error("RabbitMQPush Failed to declare a queue",zap.Error(err))
		return
	}


	// 发布一条消息到队列中
	err = ch.Publish(
		"",     // 交换器
		q.Name, // 路由键
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(msg),
		})
	if err != nil {
		log.Error("RabbitMQPush Failed to publish a message",zap.Error(err))
		return
	}
	log.Info("RabbitMQPush send success",zap.Any("msg",msg))
}
