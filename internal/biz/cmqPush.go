package biz

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/log"
	"fmt"
	uuid "github.com/satori/go.uuid"
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
