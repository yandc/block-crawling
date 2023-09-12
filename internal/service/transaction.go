package service

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"go.uber.org/zap"
)

type TransactionService struct {
	pb.UnimplementedTransactionServer
	ts *biz.TransactionUsecase
}

func NewTransactionService(ts *biz.TransactionUsecase, p platform.Server, ip platform.InnerPlatformContainer) *TransactionService {
	return &TransactionService{ts: ts}
}

func (s *TransactionService) CreateRecordFromWallet(ctx context.Context, req *pb.TransactionReq) (*pb.CreateResponse, error) {
	log.Info("request", zap.Any("request", req))
	biz.ChainTypeAdd(req.ChainName)
	subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result, err := s.ts.CreateRecordFromWallet(subctx, req)
	return result, err
}

func (s *TransactionService) PageLists(ctx context.Context, req *pb.PageListRequest) (*pb.PageListResponse, error) {
	subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	chainType := biz.ChainTypeAdd(req.ChainName)[req.ChainName]
	if req.Platform == biz.WEB {
		req.TransactionTypeNotInList = []string{biz.EVENTLOG}
	} else if req.Platform == biz.ANDROID {
		if req.OsVersion > 2022101201 {
			req.TransactionTypeNotInList = []string{biz.EVENTLOG}
		} else {
			req.TransactionTypeNotInList = []string{biz.CONTRACT}
		}

		if req.OsVersion < 2023052301 {
			req.TransactionTypeNotInList = append(req.TransactionTypeNotInList, []string{biz.APPROVENFT, biz.TRANSFERNFT}...)
		}
	} else if req.Platform == biz.IOS {
		if req.OsVersion >= 2022101501 {
			req.TransactionTypeNotInList = []string{biz.EVENTLOG}
		} else {
			req.TransactionTypeNotInList = []string{biz.CONTRACT}
		}

		if req.OsVersion < 2023052301 {
			req.TransactionTypeNotInList = append(req.TransactionTypeNotInList, []string{biz.APPROVENFT, biz.TRANSFERNFT}...)
		}
	}
	for _, transactionType := range req.TransactionTypeList {
		if transactionType == biz.OTHER {
			req.TransactionTypeList = append(req.TransactionTypeList, biz.CONTRACT, biz.CREATEACCOUNT, biz.CLOSEACCOUNT, biz.REGISTERTOKEN,
				biz.DIRECTTRANSFERNFTSWITCH, biz.CREATECONTRACT, biz.MINT, biz.SWAP)
			break
		}
	}

	if req.OrderBy == "" {
		req.OrderBy = "tx_time desc"
	}

	if req.PageSize <= 0 {
		req.PageSize = data.PAGE_SIZE
	} else if req.PageSize > data.MAX_PAGE_SIZE {
		req.PageSize = data.MAX_PAGE_SIZE
	}

	if len(req.StatusList) > 0 {
		for _, status := range req.StatusList {
			if status == biz.PENDING {
				req.StatusList = append(req.StatusList, biz.NO_STATUS)
			} else if status == biz.FAIL {
				req.StatusList = append(req.StatusList, biz.DROPPED_REPLACED, biz.DROPPED)
			}
		}
	}

	result, err := s.ts.PageList(subctx, req)
	if result != nil && len(result.List) > 0 {
		for _, record := range result.List {
			if record == nil {
				continue
			}

			if record.Status == biz.DROPPED_REPLACED || record.Status == biz.DROPPED {
				record.Status = biz.FAIL
			}
			if record.Status == biz.NO_STATUS {
				record.Status = biz.PENDING
			}
			for _, operateRecord := range record.OperateRecordList {
				if operateRecord.Status == biz.DROPPED_REPLACED || operateRecord.Status == biz.DROPPED {
					operateRecord.Status = biz.FAIL
				}
				if operateRecord.Status == biz.NO_STATUS {
					operateRecord.Status = biz.PENDING
				}
			}

			if record.TransactionType == biz.APPROVE || record.TransactionType == biz.APPROVENFT {
				if record.Amount == "" || record.Amount == "0" {
					var data = record.Data
					if data == "" {
						if record.ClientData != "" {
							clientData := make(map[string]interface{})
							if jsonErr := json.Unmarshal([]byte(record.ClientData), &clientData); jsonErr == nil {
								dappTxinfoMap, dok := clientData["dappTxinfo"].(map[string]interface{})
								if dok {
									data, _ = dappTxinfoMap["data"].(string)
								}
							}
						}
					}

					if data == "" {
						if record.TransactionType == biz.APPROVE {
							record.TransactionType = biz.CANCELAPPROVE
						} else if record.TransactionType == biz.APPROVENFT {
							record.TransactionType = biz.CANCELAPPROVENFT
						}
					} else {
						switch chainType {
						case biz.EVM, biz.TVM:
							if len(data) == 136 && strings.HasSuffix(data, "0000000000000000000000000000000000000000000000000000000000000000") {
								if record.TransactionType == biz.APPROVE {
									record.TransactionType = biz.CANCELAPPROVE
								} else if record.TransactionType == biz.APPROVENFT {
									record.TransactionType = biz.CANCELAPPROVENFT
								}
							}
						}
					}
				}
			}
			for _, operateRecord := range record.OperateRecordList {
				if operateRecord.TransactionType == biz.APPROVE || operateRecord.TransactionType == biz.APPROVENFT {
					if operateRecord.Amount == "" || operateRecord.Amount == "0" {
						var data = operateRecord.Data
						if data == "" {
							if operateRecord.ClientData != "" {
								clientData := make(map[string]interface{})
								if jsonErr := json.Unmarshal([]byte(operateRecord.ClientData), &clientData); jsonErr == nil {
									dappTxinfoMap, dok := clientData["dappTxinfo"].(map[string]interface{})
									if dok {
										data, _ = dappTxinfoMap["data"].(string)
									}
								}
							}
						}

						if data == "" {
							if operateRecord.TransactionType == biz.APPROVE {
								operateRecord.TransactionType = biz.CANCELAPPROVE
							} else if operateRecord.TransactionType == biz.APPROVENFT {
								operateRecord.TransactionType = biz.CANCELAPPROVENFT
							}
						} else {
							switch chainType {
							case biz.EVM, biz.TVM:
								if len(data) == 136 && strings.HasSuffix(data, "0000000000000000000000000000000000000000000000000000000000000000") {
									if operateRecord.TransactionType == biz.APPROVE {
										operateRecord.TransactionType = biz.CANCELAPPROVE
									} else if operateRecord.TransactionType == biz.APPROVENFT {
										operateRecord.TransactionType = biz.CANCELAPPROVENFT
									}
								}
							}
						}
					}
				}
			}

			if req.Platform == biz.ANDROID || req.Platform == biz.IOS {
				if req.OsVersion < 2023040601 {
					if record.TransactionType != biz.CONTRACT || record.EventLog == "" {
						continue
					}
					if platInfo, ok := biz.GetChainPlatInfo(req.ChainName); ok {
						mainDecimals := platInfo.Decimal
						mainSymbol := platInfo.NativeCurrency
						eventLogStr := record.EventLog
						var eventLogs []*types.EventLogUid
						err = json.Unmarshal([]byte(eventLogStr), &eventLogs)
						if err != nil {
							continue
						}
						for _, eventLog := range eventLogs {
							if eventLog.Token.Address == "" {
								eventLog.Token.Address = "0x0000000000000000000000000000000000000000"
								eventLog.Token.Amount = eventLog.Amount.String()
								eventLog.Token.Decimals = int64(mainDecimals)
								eventLog.Token.Symbol = mainSymbol
							}
						}
						eventLogStr, _ = utils.JsonEncode(eventLogs)
						record.EventLog = eventLogStr
					}
				}

				if req.OsVersion < 2023052301 {
					if record.TransactionType != biz.CONTRACT || record.EventLog == "" {
						continue
					}
					eventLogStr := record.EventLog
					var eventLogs []*types.EventLogUid
					var newEventLogs []*types.EventLogUid
					err = json.Unmarshal([]byte(eventLogStr), &eventLogs)
					if err != nil {
						continue
					}
					for _, eventLog := range eventLogs {
						if eventLog.Token.TokenType == "" || eventLog.Token.TokenType == biz.ERC20 {
							newEventLogs = append(newEventLogs, eventLog)
						}
					}
					if newEventLogs != nil {
						eventLogStr, _ = utils.JsonEncode(newEventLogs)
					} else {
						eventLogStr = ""
					}
					record.EventLog = eventLogStr
				}
			}
		}
	}
	return result, err
}

func (s *TransactionService) PageList(ctx context.Context, req *pb.PageListRequest) (*pb.PageListResponse, error) {
	subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	chainType := biz.ChainTypeAdd(req.ChainName)[req.ChainName]
	if req.Platform == biz.WEB {
		req.TransactionTypeNotInList = []string{biz.EVENTLOG}
	} else if req.Platform == biz.ANDROID {
		if req.OsVersion > 2022101201 {
			req.TransactionTypeNotInList = []string{biz.EVENTLOG}
		} else {
			req.TransactionTypeNotInList = []string{biz.CONTRACT}
		}

		if req.OsVersion < 2023052301 {
			req.TransactionTypeNotInList = append(req.TransactionTypeNotInList, []string{biz.APPROVENFT, biz.TRANSFERNFT}...)
		}
	} else if req.Platform == biz.IOS {
		if req.OsVersion >= 2022101501 {
			req.TransactionTypeNotInList = []string{biz.EVENTLOG}
		} else {
			req.TransactionTypeNotInList = []string{biz.CONTRACT}
		}

		if req.OsVersion < 2023052301 {
			req.TransactionTypeNotInList = append(req.TransactionTypeNotInList, []string{biz.APPROVENFT, biz.TRANSFERNFT}...)
		}
	}
	for _, transactionType := range req.TransactionTypeList {
		if transactionType == biz.OTHER {
			req.TransactionTypeList = append(req.TransactionTypeList, biz.CONTRACT, biz.CREATEACCOUNT, biz.CLOSEACCOUNT, biz.REGISTERTOKEN,
				biz.DIRECTTRANSFERNFTSWITCH, biz.CREATECONTRACT, biz.MINT, biz.SWAP)
			break
		}
	}

	if req.OrderBy == "" {
		req.OrderBy = "tx_time desc"
	}

	if req.PageSize <= 0 {
		req.PageSize = data.PAGE_SIZE
	} else if req.PageSize > data.MAX_PAGE_SIZE {
		req.PageSize = data.MAX_PAGE_SIZE
	}

	if len(req.StatusList) > 0 {
		for _, status := range req.StatusList {
			if status == biz.PENDING {
				req.StatusList = append(req.StatusList, biz.NO_STATUS)
			} else if status == biz.FAIL {
				req.StatusList = append(req.StatusList, biz.DROPPED_REPLACED, biz.DROPPED)
			}
		}
	}

	result, err := s.ts.ClientPageList(subctx, req)
	if result != nil && len(result.List) > 0 {
		for _, record := range result.List {
			if record == nil {
				continue
			}

			if record.Status == biz.DROPPED_REPLACED || record.Status == biz.DROPPED {
				record.Status = biz.FAIL
			}
			if record.Status == biz.NO_STATUS {
				record.Status = biz.PENDING
			}
			for _, operateRecord := range record.OperateRecordList {
				if operateRecord.Status == biz.DROPPED_REPLACED || operateRecord.Status == biz.DROPPED {
					operateRecord.Status = biz.FAIL
				}
				if operateRecord.Status == biz.NO_STATUS {
					operateRecord.Status = biz.PENDING
				}
			}

			if record.TransactionType == biz.APPROVE || record.TransactionType == biz.APPROVENFT {
				if record.Amount == "" || record.Amount == "0" {
					var data = record.Data
					if data == "" {
						if record.ClientData != "" {
							clientData := make(map[string]interface{})
							if jsonErr := json.Unmarshal([]byte(record.ClientData), &clientData); jsonErr == nil {
								dappTxinfoMap, dok := clientData["dappTxinfo"].(map[string]interface{})
								if dok {
									data, _ = dappTxinfoMap["data"].(string)
								}
							}
						}
					}

					if data == "" {
						if record.TransactionType == biz.APPROVE {
							record.TransactionType = biz.CANCELAPPROVE
						} else if record.TransactionType == biz.APPROVENFT {
							record.TransactionType = biz.CANCELAPPROVENFT
						}
					} else {
						switch chainType {
						case biz.EVM, biz.TVM:
							if len(data) == 136 && strings.HasSuffix(data, "0000000000000000000000000000000000000000000000000000000000000000") {
								if record.TransactionType == biz.APPROVE {
									record.TransactionType = biz.CANCELAPPROVE
								} else if record.TransactionType == biz.APPROVENFT {
									record.TransactionType = biz.CANCELAPPROVENFT
								}
							}
						}
					}
				}
			}
			for _, operateRecord := range record.OperateRecordList {
				if operateRecord.TransactionType == biz.APPROVE || operateRecord.TransactionType == biz.APPROVENFT {
					if operateRecord.Amount == "" || operateRecord.Amount == "0" {
						var data = operateRecord.Data
						if data == "" {
							if operateRecord.ClientData != "" {
								clientData := make(map[string]interface{})
								if jsonErr := json.Unmarshal([]byte(operateRecord.ClientData), &clientData); jsonErr == nil {
									dappTxinfoMap, dok := clientData["dappTxinfo"].(map[string]interface{})
									if dok {
										data, _ = dappTxinfoMap["data"].(string)
									}
								}
							}
						}

						if data == "" {
							if operateRecord.TransactionType == biz.APPROVE {
								operateRecord.TransactionType = biz.CANCELAPPROVE
							} else if operateRecord.TransactionType == biz.APPROVENFT {
								operateRecord.TransactionType = biz.CANCELAPPROVENFT
							}
						} else {
							switch chainType {
							case biz.EVM, biz.TVM:
								if len(data) == 136 && strings.HasSuffix(data, "0000000000000000000000000000000000000000000000000000000000000000") {
									if operateRecord.TransactionType == biz.APPROVE {
										operateRecord.TransactionType = biz.CANCELAPPROVE
									} else if operateRecord.TransactionType == biz.APPROVENFT {
										operateRecord.TransactionType = biz.CANCELAPPROVENFT
									}
								}
							}
						}
					}
				}
			}

			if req.Platform == biz.ANDROID || req.Platform == biz.IOS {
				if req.OsVersion < 2023040601 {
					if record.TransactionType != biz.CONTRACT || record.EventLog == "" {
						continue
					}
					if platInfo, ok := biz.GetChainPlatInfo(req.ChainName); ok {
						mainDecimals := platInfo.Decimal
						mainSymbol := platInfo.NativeCurrency
						eventLogStr := record.EventLog
						var eventLogs []*types.EventLogUid
						err = json.Unmarshal([]byte(eventLogStr), &eventLogs)
						if err != nil {
							continue
						}
						for _, eventLog := range eventLogs {
							if eventLog.Token.Address == "" {
								eventLog.Token.Address = "0x0000000000000000000000000000000000000000"
								eventLog.Token.Amount = eventLog.Amount.String()
								eventLog.Token.Decimals = int64(mainDecimals)
								eventLog.Token.Symbol = mainSymbol
							}
						}
						eventLogStr, _ = utils.JsonEncode(eventLogs)
						record.EventLog = eventLogStr
					}
				}

				if req.OsVersion < 2023052301 {
					if record.TransactionType != biz.CONTRACT || record.EventLog == "" {
						continue
					}
					eventLogStr := record.EventLog
					var eventLogs []*types.EventLogUid
					var newEventLogs []*types.EventLogUid
					err = json.Unmarshal([]byte(eventLogStr), &eventLogs)
					if err != nil {
						continue
					}
					for _, eventLog := range eventLogs {
						if eventLog.Token.TokenType == "" || eventLog.Token.TokenType == biz.ERC20 {
							newEventLogs = append(newEventLogs, eventLog)
						}
					}
					if newEventLogs != nil {
						eventLogStr, _ = utils.JsonEncode(newEventLogs)
					} else {
						eventLogStr = ""
					}
					record.EventLog = eventLogStr
				}
			}
		}
	}
	return result, err
}

func (s *TransactionService) GetAmount(ctx context.Context, req *pb.AmountRequest) (*pb.AmountResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	result, err := s.ts.GetAmount(ctx, req)
	return result, err
}

func (s *TransactionService) GetDappList(ctx context.Context, req *pb.DappListReq) (*pb.DappListResp, error) {
	biz.ChainTypeAdd(req.ChainName)
	subctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if req.Platform == biz.ANDROID || req.Platform == biz.IOS {
		if req.OsVersion <= 2023041001 {
			req.DappType = biz.APPROVE
		}
	}
	result, err := s.ts.GetDappList(subctx, req)
	return result, err
}

func (s *TransactionService) GetAllOpenAmount(ctx context.Context, req *pb.OpenAmountReq) (*pb.OpenAmoutResp, error) {
	biz.ChainTypeAdd(req.ChainName)
	subctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	result, err := s.ts.GetAllOpenAmount(subctx, req)
	return result, err
}

func (s *TransactionService) GetNonce(ctx context.Context, req *pb.NonceReq) (*pb.NonceResp, error) {
	biz.ChainTypeAdd(req.ChainName)
	subctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	result, err := s.ts.GetNonce(subctx, req)
	return result, err
}

func (s *TransactionService) GetDappListPageList(ctx context.Context, req *pb.DappPageListReq) (*pb.DappPageListResp, error) {
	biz.ChainTypeAdd(req.ChainName)
	subctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	if req.Platform == biz.ANDROID || req.Platform == biz.IOS {
		if req.OsVersion <= 2023041001 {
			req.DappType = biz.APPROVE
		}
	}
	if req.Page == 0 {
		req.Page = 1
	}
	if req.Limit == 0 {
		req.Limit = 20
	}

	result, err := s.ts.GetDappListPageList(subctx, req)
	return result, err
}

func (s *TransactionService) PageListAsset(ctx context.Context, req *pb.PageListAssetRequest) (*pb.PageListAssetResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	if req.Uid == "" && len(req.AddressList) == 0 {
		return nil, errors.New("uid or addressList is required")
	}
	if req.Currency != "CNY" && req.Currency != "USD" {
		return nil, errors.New("currency must be CNY or USD")
	}

	if req.OrderBy == "" {
		req.OrderBy = "id desc"
	}

	if req.PageSize <= 0 {
		req.PageSize = data.PAGE_SIZE
	} else if req.PageSize > data.MAX_PAGE_SIZE {
		req.PageSize = data.MAX_PAGE_SIZE
	}

	result, err := s.ts.PageListAsset(ctx, req)
	return result, err
}

func (s *TransactionService) PageListAssetGroup(ctx context.Context, req *pb.PageListAssetRequest) (*pb.PageListAssetResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	if req.OrderBy == "" {
		req.OrderBy = "currencyAmount desc"
	}

	if req.PageSize <= 0 {
		req.PageSize = data.PAGE_SIZE
	} else if req.PageSize > data.MAX_PAGE_SIZE {
		req.PageSize = data.MAX_PAGE_SIZE
	}

	result, err := s.ts.PageListAssetGroup(ctx, req)
	return result, err
}

func (s *TransactionService) ClientPageListAsset(ctx context.Context, req *pb.PageListAssetRequest) (*pb.PageListAssetResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	if req.ChainName == "" {
		return nil, errors.New("chainName is required")
	}
	if req.Uid == "" && len(req.AddressList) == 0 {
		return nil, errors.New("uid or addressList is required")
	}
	if req.Currency != "CNY" && req.Currency != "USD" {
		return nil, errors.New("currency must be CNY or USD")
	}

	if req.OrderBy == "" {
		req.OrderBy = "currencyAmount desc"
	}

	if req.PageSize <= 0 {
		req.PageSize = data.PAGE_SIZE
	} else if req.PageSize > data.MAX_PAGE_SIZE {
		req.PageSize = data.MAX_PAGE_SIZE
	}

	result, err := s.ts.ClientPageListAsset(ctx, req)
	return result, err
}

func (s *TransactionService) GetBalance(ctx context.Context, req *pb.AssetRequest) (*pb.ListBalanceResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	result, err := s.ts.GetBalance(ctx, req)
	return result, err
}

func (s *TransactionService) ListAmountUidDimension(ctx context.Context, req *pb.ListAmountUidDimensionRequest) (*pb.ListAmountUidDimensionResponse, error) {
	if len(req.UidList) == 0 {
		return nil, errors.New("uidList is required")
	}
	if req.Currency != "CNY" && req.Currency != "USD" {
		return nil, errors.New("currency must be CNY or USD")
	}

	result, err := s.ts.ListAmountUidDimension(ctx, req)
	return result, err
}

func (s *TransactionService) ListHasBalanceUidDimension(ctx context.Context, req *pb.ListHasBalanceUidDimensionRequest) (*pb.ListHasBalanceUidDimensionResponse, error) {
	if len(req.UidList) == 0 {
		return nil, errors.New("uidList is required")
	}

	result, err := s.ts.ListHasBalanceUidDimension(ctx, req)
	return result, err
}

func (s *TransactionService) ListHasBalanceDimension(ctx context.Context, req *pb.ListHasBalanceDimensionRequest) (*pb.ListHasBalanceDimensionResponse, error) {
	if len(req.UidList) == 0 && len(req.AddressList) == 0 {
		return nil, errors.New("uidList or addressList is required")
	}
	if req.GroupBy != "uid" && req.GroupBy != "address" {
		return nil, errors.New("groupBy must be uid or address")
	}

	result, err := s.ts.ListHasBalanceDimension(ctx, req)
	return result, err
}

func (s *TransactionService) AssetHistoryFundAmount(ctx context.Context, req *pb.AssetHistoryRequest) (*pb.AssetHistoryFundAmountListResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	if req.StartTime >= req.StopTime {
		return nil, errors.New("startTime is greater than stopTime")
	}
	if req.ChainName == "" {
		req.ChainName = "all"
	}
	if req.AddressType == 0 {
		req.AddressType = -1
	}
	result, err := s.ts.AssetHistoryFundAmount(ctx, req)
	return result, err
}

func (s *TransactionService) AssetHistoryAddressAmount(ctx context.Context, req *pb.AssetHistoryRequest) (*pb.AssetHistoryAddressAmountListResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	if req.StartTime >= req.StopTime {
		return nil, errors.New("startTime is greater than stopTime")
	}
	if req.ChainName == "" {
		req.ChainName = "all"
	}
	if req.AddressType == 0 {
		req.AddressType = -1
	}
	result, err := s.ts.AssetHistoryAddressAmount(ctx, req)
	return result, err
}

func (s *TransactionService) ClientPageListNftAssetGroup(ctx context.Context, req *pb.PageListNftAssetRequest) (*pb.ClientPageListNftAssetGroupResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	if req.Uid == "" && len(req.AddressList) == 0 {
		return nil, errors.New("uid or addressList is required")
	}

	if req.OrderBy == "" {
		req.OrderBy = "token_id_amount desc"
	}

	if req.PageSize <= 0 {
		req.PageSize = data.PAGE_SIZE
	} else if req.PageSize > data.MAX_PAGE_SIZE {
		req.PageSize = data.MAX_PAGE_SIZE
	}

	result, err := s.ts.ClientPageListNftAssetGroup(ctx, req)
	return result, err
}

func (s *TransactionService) ClientPageListNftAsset(ctx context.Context, req *pb.PageListNftAssetRequest) (*pb.ClientPageListNftAssetResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	if req.Uid == "" && len(req.AddressList) == 0 {
		return nil, errors.New("uid or addressList is required")
	}

	if req.OrderBy == "" {
		req.OrderBy = "id desc"
	}

	if req.PageSize <= 0 {
		req.PageSize = data.PAGE_SIZE
	} else if req.PageSize > data.MAX_PAGE_SIZE {
		req.PageSize = data.MAX_PAGE_SIZE
	}

	result, err := s.ts.ClientPageListNftAsset(ctx, req)
	return result, err
}

func (s *TransactionService) GetNftBalance(ctx context.Context, req *pb.NftAssetRequest) (*pb.NftBalanceResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	result, err := s.ts.GetNftBalance(ctx, req)
	return result, err
}

func (s *TransactionService) PageListStatistic(ctx context.Context, req *pb.PageListStatisticRequest) (*pb.PageListStatisticResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	if req.StartTime >= req.StopTime {
		return nil, errors.New("startTime is greater than stopTime")
	}
	result, err := s.ts.PageListStatistic(ctx, req)
	return result, err
}

func (s *TransactionService) StatisticFundAmount(ctx context.Context, req *pb.StatisticFundRequest) (*pb.FundAmountListResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	if req.StartTime >= req.StopTime {
		return nil, errors.New("startTime is greater than stopTime")
	}
	result, err := s.ts.StatisticFundAmount(ctx, req)
	return result, err
}

func (s *TransactionService) StatisticFundRate(ctx context.Context, req *pb.StatisticFundRequest) (*pb.FundRateListResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	if req.StartTime >= req.StopTime {
		return nil, errors.New("startTime is greater than stopTime")
	}
	result, err := s.ts.StatisticFundRate(ctx, req)
	return result, err
}
func (s *TransactionService) GetUnspentTx(ctx context.Context, req *pb.UnspentReq) (*pb.UnspentResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	subctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	result, err := s.ts.GetUnspentTx(subctx, req)
	return result, err
}
func (s *TransactionService) GetNftRecord(ctx context.Context, req *pb.NftRecordReq) (*pb.NftRecordResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	subctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	result, err := s.ts.GetNftRecord(subctx, req)
	return result, err
}

func (s *TransactionService) JsonRpc(ctx context.Context, req *pb.JsonReq) (*pb.JsonResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	started := time.Now()
	subctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	result, err := s.ts.JsonRpc(subctx, req)
	log.Info(
		"JsonRpc",
		zap.String("method", req.Method),
		zap.String("uid", req.Uid),
		zap.String("chainName", req.ChainName),
		zap.String("params", req.Params),
		zap.String("device", req.Device),
		zap.Duration("elapsed", time.Now().Sub(started)),
		zap.Any("response", result),
		zap.Error(err),
	)
	return result, err
}

func (s *TransactionService) KanbanSummary(ctx context.Context, req *pb.KanbanSummaryRequest) (*pb.KanbanSummaryResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	log.Info("KanbanSummary", zap.Any("request", req))
	subctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	return s.ts.KanbanSummary(subctx, req)
}

// 看板交易数据
func (s *TransactionService) KanbanTxChart(ctx context.Context, req *pb.KanbanChartRequest) (*pb.KanbanChartResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	log.Info("KanbanTxChart", zap.Any("request", req))
	subctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	return s.ts.KanbanTxChart(subctx, req)
}

// 看板合约数据
func (s *TransactionService) KanbanContractChart(ctx context.Context, req *pb.KanbanChartRequest) (*pb.KanbanChartResponse, error) {
	biz.ChainTypeAdd(req.ChainName)
	log.Info("KanbanContractChart", zap.Any("request", req))
	subctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	return s.ts.KanbanContractChart(subctx, req)
}
