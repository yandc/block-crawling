package scheduling

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
)

type FailureKind string

const (
	// {"Message":"[/Users/bixin/go/src/mili-chaindata/platform/aptos/client.go]:113  {\"message\":\"Account not found by Address(0x4c0de061437ab4303d2dff38843c51a1311e12eac191d0b9c1b70c429cb90fb6) and Ledger version(413278229)\",\"error_code\":\"account_not_found\",\"vm_error_code\":null}","Custom":"network error"}
	// invalid opcode: INVALID
	// {"Message":"[/Users/bixin/go/src/mili-chaindata/platform/utils/utils.go]:99  credits limited to 330/sec","Custom":"request blockchain failed"}
	FailureKindUnknown FailureKind = "Unclassfied"

	// Post "https://virginia.rpc.blxrbdn.com": net/http: fetch() failed: Failed to fetch
	// Post "https://bsc-dataseed3.binance.org": dial tcp [2a03:2880:f136:83:face:b00c:0:25de]:443: i/o timeout
	// Post "https://rpc.ankr.com/polygon": dial tcp [2606:4700:4400::6812:278c]:443: connect: network is unreachable
	// {"Message":"[/Users/bixin/go/src/mili-chaindata/platform/tron/trongridClient.go]:118  Get \"https://api.trongrid.io/wallet/getnowblock\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)","Custom":"network error"}
	// Post "https://arb1.arbitrum.io/rpc": dial tcp: lookup arb1.arbitrum.io: no such host
	FailureKindNetwork FailureKind = "Network"

	// balance=0,utxoAllBalance=0,utxo amount greater than address balance or utxo is nil
	// balance=19997514,utxoAllBalance=0,获取utxo为空.
	FailureKindUTXO FailureKind = "UTXO"

	// execution reverted: ERC20: transfer amount exceeds balance
	// execution reverted: ERC1155: insufficient balance for transfer
	FailureKindBalance FailureKind = "Balance"

	FailureKindSign  FailureKind = "Sign"
	FailureKindNonce FailureKind = "Nonce"
	FailureKindGas   FailureKind = "Gas"
)

type ReportFailureTxnsTask struct {
}

type failureCounter map[string]map[FailureKind]uint64

// Run implements cron.Job
func (*ReportFailureTxnsTask) Run() {
	now := time.Now()
	endTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()).Unix()
	var aWeekSecs int64 = 3600 * 24 * 7
	beginTime := endTime - aWeekSecs*2
	pageSize := 1000
	var cursor int64

	currentWeekCounters := make(failureCounter)
	previousWeekCounters := make(failureCounter)

	for {
		records, err := data.UserSendRawHistoryRepoInst.CursorListAllDesc(context.Background(), &cursor, pageSize)
		if err != nil {
			log.Error("LOAD SEND RAW HISTORIES ERROR", zap.Error(err), zap.Int64("cursor", cursor))
			return
		}
		if len(records) == 0 {
			break
		}
		for _, record := range records {
			if record.CreatedAt > endTime {
				continue
			}
			if record.CreatedAt < beginTime {
				goto _GENERATE
			}
			// 1,2 [1:交易签名][2:信息签名]
			if record.SignType != "1" || record.ErrMsg == "" {
				continue
			}
			var counters failureCounter
			if endTime-record.CreatedAt < aWeekSecs {
				counters = currentWeekCounters
			} else {
				counters = previousWeekCounters
			}
			if _, ok := counters[record.ChainName]; !ok {
				counters[record.ChainName] = make(map[FailureKind]uint64)
			}
			innerCounter := counters[record.ChainName]
			kind := classify(record.ChainName, record.ErrMsg)
			if _, ok := innerCounter[kind]; !ok {
				innerCounter[kind] = 0
			}
			innerCounter[kind]++
		}
	}
_GENERATE:
	chainNames := make(map[string]bool)
	kinds := make(map[FailureKind]bool)
	for chainName, counters := range currentWeekCounters {
		chainNames[chainName] = true
		for k := range counters {
			kinds[k] = true
		}
	}

	for chainName, counters := range previousWeekCounters {
		chainNames[chainName] = true
		for k := range counters {
			kinds[k] = true
		}
	}
	counters := make(map[FailureKind][2]uint64)
	for chainName := range chainNames {
		currentCounters := currentWeekCounters[chainName]
		prevCounters := previousWeekCounters[chainName]
		for kind := range kinds {
			current := currentCounters[kind]
			prev := prevCounters[kind]
			if _, ok := counters[kind]; !ok {
				counters[kind] = [2]uint64{0, 0}
			}
			val := counters[kind]
			counters[kind] = [2]uint64{val[0] + current, val[1] + prev}
		}
	}

	content := make([][]biz.Content, 0, 16)

	for kind, val := range counters {
		if kind == FailureKindUnknown {
			continue
		}
		current := val[0]
		prev := val[1]
		if current == 0 && prev == 0 {
			continue
		}

		ratio := (float64(current) - float64(prev)) / float64(prev) * 100
		content = append(content, []biz.Content{
			{
				Tag:  "text",
				Text: gettext(kind),
			},
		}, []biz.Content{
			{
				Tag:  "text",
				Text: fmt.Sprintf("\t上周：%d", prev),
			},
		}, []biz.Content{
			{
				Tag:  "text",
				Text: fmt.Sprintf("\t本周：%d", current),
			},
		}, []biz.Content{
			{
				Tag:  "text",
				Text: fmt.Sprintf("\t环比：%.2f%%", ratio),
			},
		})
	}

	biz.LarkClient.SendRichText(
		"reportFailure",
		"失败统计",
		content,
	)
}

func classify(chainName string, message string) FailureKind {
	msg := strings.ToLower(message)
	if strings.Contains(msg, "utxo") {
		return FailureKindUTXO
	} else if (strings.Contains(msg, "balance") || strings.Contains(msg, "funds")) &&
		(strings.Contains(msg, "exceed") || strings.Contains(msg, "insufficient")) {
		return FailureKindBalance
	} else if strings.Contains(msg, "sign error") {
		return FailureKindSign
	} else {
		networkKeywords := [][]string{
			{"fetch()", "failed"},
			{"i/o", "timeout"},
			{"network is unreachable"},
			{"client.timeout"},
			{"dial", "no such host"},
		}
		if isMatchKeyword(msg, networkKeywords) {
			return FailureKindNetwork
		}

		nonceKeywords := [][]string{
			{"nonce too high"},
		}
		if isMatchKeyword(msg, nonceKeywords) {
			return FailureKindNonce
		}

		gasKeywords := [][]string{
			{"gas too low,"},
			{"transaction underpriced"},
			{"enough", "gas"},
		}
		if isMatchKeyword(msg, gasKeywords) {
			return FailureKindGas
		}
	}
	log.Info("FAILURE CLASSIFICATION NONE", zap.String("chainName", chainName), zap.String("msg", msg))
	return FailureKindUnknown
}

func isMatchKeyword(msg string, keywords [][]string) bool {
	for _, keywords := range keywords {
		matched := true
		for _, k := range keywords {
			matched = matched && strings.Contains(msg, k)
		}
		if matched {
			return true
		}
	}
	return false
}

func gettext(kind FailureKind) string {
	switch kind {
	case FailureKindSign:
		return "签名失败（Sign error）"
	case FailureKindNetwork:
		return "网络失败（Network error）"
	case FailureKindUTXO:
		return "UTXO"
	case FailureKindBalance:
		return "余额不足（Insufficient balance）"
	case FailureKindNonce:
		return "Nonce（Low/High）"
	case FailureKindGas:
		return "手续费过低（Gas too low）"
	}
	return "用户侧导致的失败"
}
