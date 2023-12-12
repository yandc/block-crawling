package biz

import (
	v1 "block-crawling/api/bfstation/client"
	"block-crawling/internal/log"
	"context"
	"fmt"
	"math"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func PushBFStationSwapEvent(ctx context.Context, event *v1.SyncSwapEventRequest) error {
	log.Info("PUSH EVENT TO BFSTATION", zap.Any("event", event))
	client, fn, err := dialBfstation()
	if err != nil {
		return fmt.Errorf("[dialBfstation] %w", err)
	}
	defer fn()
	_, err = client.SyncSwapEvent(ctx, event)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Second * time.Duration(math.Pow(2, float64(i))))
		_, err = client.SyncSwapEvent(ctx, event)
	}
	if err != nil {
		return fmt.Errorf("[SyncSwapEvent] %w", err)
	}
	return nil
}

func PushBFStationLiquidityEvent(ctx context.Context, event *v1.SyncLiquidityEventRequest) error {
	log.Info("PUSH EVENT TO BFSTATION", zap.Any("event", event))
	client, fn, err := dialBfstation()
	if err != nil {
		return fmt.Errorf("[dialBfstation] %w", err)
	}
	defer fn()
	_, err = client.SyncLiquidityEvent(ctx, event)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Second * time.Duration(math.Pow(2, float64(i))))
		_, err = client.SyncLiquidityEvent(ctx, event)
	}
	if err != nil {
		return fmt.Errorf("[SyncLiquidityEvent] %w", err)
	}
	return nil
}

func dialBfstation() (v1.BFStationClient, func(), error) {
	conn, err := grpc.Dial(AppConfig.BfstationRpc, grpc.WithInsecure())
	if err != nil {
		return nil, nil, fmt.Errorf("[%s] %w", AppConfig.BfstationRpc, err)
	}
	return v1.NewBFStationClient(conn), func() {
		conn.Close()
	}, nil
}
