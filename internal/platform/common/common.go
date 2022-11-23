package common

import (
	"block-crawling/internal/biz"
	"fmt"

	"gitlab.bixin.com/mili/node-driver/chain"
)

// NotifyForkedDelete notify lark when delete rows when forked.
func NotifyForkedDelete(chainName string, blockNumber uint64, nRows int64) {
	if nRows <= 0 {
		return
	}
	alarmMsg := fmt.Sprintf("请注意：%s 链产出分叉，回滚到块高 %d，删除 %d 条数据", chainName, blockNumber, nRows)
	alarmOpts := biz.WithMsgLevel("FATAL")
	biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
}

func NotifyForkedError(chainName string, err error) bool {
	if err == nil {
		return false
	}

	alarmOpts := biz.WithMsgLevel("FATAL")
	var alarmMsg string
	if err == chain.ErrForkedZeroBlockNumber {
		alarmMsg = fmt.Sprintf("请注意： %s 链产生分叉，但是获取块高为 0", chainName)
	} else if err, ok := err.(*chain.ForkDeltaOverflow); ok {
		alarmMsg = fmt.Sprintf("请注意： %s 链产生分叉，但是回滚到了安全块高以外，链上块高：%d，回滚到块高：%d，安全块高差：%d", chainName, err.ChainHeight, err.BlockNumber, err.SafelyDelta)
	} else {
		return false
	}
	biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	return true
}
