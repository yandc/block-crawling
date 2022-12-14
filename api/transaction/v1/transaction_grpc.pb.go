// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.3
// source: api/transaction/v1/transaction.proto

package v1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// TransactionClient is the client API for Transaction service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type TransactionClient interface {
	CreateRecordFromWallet(ctx context.Context, in *TransactionReq, opts ...grpc.CallOption) (*CreateResponse, error)
	//分页查询交易记录列表
	PageList(ctx context.Context, in *PageListRequest, opts ...grpc.CallOption) (*PageListResponse, error)
	//查询pending状态的总金额
	GetAmount(ctx context.Context, in *AmountRequest, opts ...grpc.CallOption) (*AmountResponse, error)
	GetDappList(ctx context.Context, in *DappListReq, opts ...grpc.CallOption) (*DappListResp, error)
	GetAllOpenAmount(ctx context.Context, in *OpenAmountReq, opts ...grpc.CallOption) (*OpenAmoutResp, error)
	GetNonce(ctx context.Context, in *NonceReq, opts ...grpc.CallOption) (*NonceResp, error)
	GetDappListPageList(ctx context.Context, in *DappPageListReq, opts ...grpc.CallOption) (*DappPageListResp, error)
	//分页查询用户资产列表
	PageListAsset(ctx context.Context, in *PageListAssetRequest, opts ...grpc.CallOption) (*PageListAssetResponse, error)
	//查询用户余额
	GetBalance(ctx context.Context, in *AssetRequest, opts ...grpc.CallOption) (*ListBalanceResponse, error)
	//客户端分页查询用户NFT资产分组列表
	ClientPageListNftAssetGroup(ctx context.Context, in *PageListNftAssetRequest, opts ...grpc.CallOption) (*ClientPageListNftAssetGroupResponse, error)
	//客户端分页查询用户NFT资产列表
	ClientPageListNftAsset(ctx context.Context, in *PageListNftAssetRequest, opts ...grpc.CallOption) (*ClientPageListNftAssetResponse, error)
	//查询用户NFT余额
	GetNftBalance(ctx context.Context, in *NftAssetRequest, opts ...grpc.CallOption) (*NftBalanceResponse, error)
	//分页查询交易数据统计列表
	PageListStatistic(ctx context.Context, in *PageListStatisticRequest, opts ...grpc.CallOption) (*PageListStatisticResponse, error)
	//交易数据看板查询金额趋势图
	StatisticFundAmount(ctx context.Context, in *StatisticFundRequest, opts ...grpc.CallOption) (*FundAmountListResponse, error)
	//交易数据看板查询金额占比图
	StatisticFundRate(ctx context.Context, in *StatisticFundRequest, opts ...grpc.CallOption) (*FundRateListResponse, error)
	//未花费资产查询
	GetUnspentTx(ctx context.Context, in *UnspentReq, opts ...grpc.CallOption) (*UnspentResponse, error)
	//后去nft流转记录
	GetNftRecord(ctx context.Context, in *NftRecordReq, opts ...grpc.CallOption) (*NftRecordResponse, error)
	//通用 接口定义
	JsonRpc(ctx context.Context, in *JsonReq, opts ...grpc.CallOption) (*JsonResponse, error)
}

type transactionClient struct {
	cc grpc.ClientConnInterface
}

func NewTransactionClient(cc grpc.ClientConnInterface) TransactionClient {
	return &transactionClient{cc}
}

func (c *transactionClient) CreateRecordFromWallet(ctx context.Context, in *TransactionReq, opts ...grpc.CallOption) (*CreateResponse, error) {
	out := new(CreateResponse)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/CreateRecordFromWallet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) PageList(ctx context.Context, in *PageListRequest, opts ...grpc.CallOption) (*PageListResponse, error) {
	out := new(PageListResponse)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/PageList", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) GetAmount(ctx context.Context, in *AmountRequest, opts ...grpc.CallOption) (*AmountResponse, error) {
	out := new(AmountResponse)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/GetAmount", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) GetDappList(ctx context.Context, in *DappListReq, opts ...grpc.CallOption) (*DappListResp, error) {
	out := new(DappListResp)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/GetDappList", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) GetAllOpenAmount(ctx context.Context, in *OpenAmountReq, opts ...grpc.CallOption) (*OpenAmoutResp, error) {
	out := new(OpenAmoutResp)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/GetAllOpenAmount", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) GetNonce(ctx context.Context, in *NonceReq, opts ...grpc.CallOption) (*NonceResp, error) {
	out := new(NonceResp)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/GetNonce", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) GetDappListPageList(ctx context.Context, in *DappPageListReq, opts ...grpc.CallOption) (*DappPageListResp, error) {
	out := new(DappPageListResp)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/GetDappListPageList", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) PageListAsset(ctx context.Context, in *PageListAssetRequest, opts ...grpc.CallOption) (*PageListAssetResponse, error) {
	out := new(PageListAssetResponse)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/PageListAsset", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) GetBalance(ctx context.Context, in *AssetRequest, opts ...grpc.CallOption) (*ListBalanceResponse, error) {
	out := new(ListBalanceResponse)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/GetBalance", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) ClientPageListNftAssetGroup(ctx context.Context, in *PageListNftAssetRequest, opts ...grpc.CallOption) (*ClientPageListNftAssetGroupResponse, error) {
	out := new(ClientPageListNftAssetGroupResponse)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/ClientPageListNftAssetGroup", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) ClientPageListNftAsset(ctx context.Context, in *PageListNftAssetRequest, opts ...grpc.CallOption) (*ClientPageListNftAssetResponse, error) {
	out := new(ClientPageListNftAssetResponse)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/ClientPageListNftAsset", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) GetNftBalance(ctx context.Context, in *NftAssetRequest, opts ...grpc.CallOption) (*NftBalanceResponse, error) {
	out := new(NftBalanceResponse)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/GetNftBalance", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) PageListStatistic(ctx context.Context, in *PageListStatisticRequest, opts ...grpc.CallOption) (*PageListStatisticResponse, error) {
	out := new(PageListStatisticResponse)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/PageListStatistic", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) StatisticFundAmount(ctx context.Context, in *StatisticFundRequest, opts ...grpc.CallOption) (*FundAmountListResponse, error) {
	out := new(FundAmountListResponse)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/StatisticFundAmount", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) StatisticFundRate(ctx context.Context, in *StatisticFundRequest, opts ...grpc.CallOption) (*FundRateListResponse, error) {
	out := new(FundRateListResponse)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/StatisticFundRate", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) GetUnspentTx(ctx context.Context, in *UnspentReq, opts ...grpc.CallOption) (*UnspentResponse, error) {
	out := new(UnspentResponse)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/GetUnspentTx", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) GetNftRecord(ctx context.Context, in *NftRecordReq, opts ...grpc.CallOption) (*NftRecordResponse, error) {
	out := new(NftRecordResponse)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/GetNftRecord", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionClient) JsonRpc(ctx context.Context, in *JsonReq, opts ...grpc.CallOption) (*JsonResponse, error) {
	out := new(JsonResponse)
	err := c.cc.Invoke(ctx, "/api.transaction.v1.Transaction/JsonRpc", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TransactionServer is the server API for Transaction service.
// All implementations must embed UnimplementedTransactionServer
// for forward compatibility
type TransactionServer interface {
	CreateRecordFromWallet(context.Context, *TransactionReq) (*CreateResponse, error)
	//分页查询交易记录列表
	PageList(context.Context, *PageListRequest) (*PageListResponse, error)
	//查询pending状态的总金额
	GetAmount(context.Context, *AmountRequest) (*AmountResponse, error)
	GetDappList(context.Context, *DappListReq) (*DappListResp, error)
	GetAllOpenAmount(context.Context, *OpenAmountReq) (*OpenAmoutResp, error)
	GetNonce(context.Context, *NonceReq) (*NonceResp, error)
	GetDappListPageList(context.Context, *DappPageListReq) (*DappPageListResp, error)
	//分页查询用户资产列表
	PageListAsset(context.Context, *PageListAssetRequest) (*PageListAssetResponse, error)
	//查询用户余额
	GetBalance(context.Context, *AssetRequest) (*ListBalanceResponse, error)
	//客户端分页查询用户NFT资产分组列表
	ClientPageListNftAssetGroup(context.Context, *PageListNftAssetRequest) (*ClientPageListNftAssetGroupResponse, error)
	//客户端分页查询用户NFT资产列表
	ClientPageListNftAsset(context.Context, *PageListNftAssetRequest) (*ClientPageListNftAssetResponse, error)
	//查询用户NFT余额
	GetNftBalance(context.Context, *NftAssetRequest) (*NftBalanceResponse, error)
	//分页查询交易数据统计列表
	PageListStatistic(context.Context, *PageListStatisticRequest) (*PageListStatisticResponse, error)
	//交易数据看板查询金额趋势图
	StatisticFundAmount(context.Context, *StatisticFundRequest) (*FundAmountListResponse, error)
	//交易数据看板查询金额占比图
	StatisticFundRate(context.Context, *StatisticFundRequest) (*FundRateListResponse, error)
	//未花费资产查询
	GetUnspentTx(context.Context, *UnspentReq) (*UnspentResponse, error)
	//后去nft流转记录
	GetNftRecord(context.Context, *NftRecordReq) (*NftRecordResponse, error)
	//通用 接口定义
	JsonRpc(context.Context, *JsonReq) (*JsonResponse, error)
	mustEmbedUnimplementedTransactionServer()
}

// UnimplementedTransactionServer must be embedded to have forward compatible implementations.
type UnimplementedTransactionServer struct {
}

func (UnimplementedTransactionServer) CreateRecordFromWallet(context.Context, *TransactionReq) (*CreateResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateRecordFromWallet not implemented")
}
func (UnimplementedTransactionServer) PageList(context.Context, *PageListRequest) (*PageListResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PageList not implemented")
}
func (UnimplementedTransactionServer) GetAmount(context.Context, *AmountRequest) (*AmountResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAmount not implemented")
}
func (UnimplementedTransactionServer) GetDappList(context.Context, *DappListReq) (*DappListResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetDappList not implemented")
}
func (UnimplementedTransactionServer) GetAllOpenAmount(context.Context, *OpenAmountReq) (*OpenAmoutResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAllOpenAmount not implemented")
}
func (UnimplementedTransactionServer) GetNonce(context.Context, *NonceReq) (*NonceResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetNonce not implemented")
}
func (UnimplementedTransactionServer) GetDappListPageList(context.Context, *DappPageListReq) (*DappPageListResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetDappListPageList not implemented")
}
func (UnimplementedTransactionServer) PageListAsset(context.Context, *PageListAssetRequest) (*PageListAssetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PageListAsset not implemented")
}
func (UnimplementedTransactionServer) GetBalance(context.Context, *AssetRequest) (*ListBalanceResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetBalance not implemented")
}
func (UnimplementedTransactionServer) ClientPageListNftAssetGroup(context.Context, *PageListNftAssetRequest) (*ClientPageListNftAssetGroupResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ClientPageListNftAssetGroup not implemented")
}
func (UnimplementedTransactionServer) ClientPageListNftAsset(context.Context, *PageListNftAssetRequest) (*ClientPageListNftAssetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ClientPageListNftAsset not implemented")
}
func (UnimplementedTransactionServer) GetNftBalance(context.Context, *NftAssetRequest) (*NftBalanceResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetNftBalance not implemented")
}
func (UnimplementedTransactionServer) PageListStatistic(context.Context, *PageListStatisticRequest) (*PageListStatisticResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PageListStatistic not implemented")
}
func (UnimplementedTransactionServer) StatisticFundAmount(context.Context, *StatisticFundRequest) (*FundAmountListResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method StatisticFundAmount not implemented")
}
func (UnimplementedTransactionServer) StatisticFundRate(context.Context, *StatisticFundRequest) (*FundRateListResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method StatisticFundRate not implemented")
}
func (UnimplementedTransactionServer) GetUnspentTx(context.Context, *UnspentReq) (*UnspentResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetUnspentTx not implemented")
}
func (UnimplementedTransactionServer) GetNftRecord(context.Context, *NftRecordReq) (*NftRecordResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetNftRecord not implemented")
}
func (UnimplementedTransactionServer) JsonRpc(context.Context, *JsonReq) (*JsonResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method JsonRpc not implemented")
}
func (UnimplementedTransactionServer) mustEmbedUnimplementedTransactionServer() {}

// UnsafeTransactionServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to TransactionServer will
// result in compilation errors.
type UnsafeTransactionServer interface {
	mustEmbedUnimplementedTransactionServer()
}

func RegisterTransactionServer(s grpc.ServiceRegistrar, srv TransactionServer) {
	s.RegisterService(&Transaction_ServiceDesc, srv)
}

func _Transaction_CreateRecordFromWallet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TransactionReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).CreateRecordFromWallet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/CreateRecordFromWallet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).CreateRecordFromWallet(ctx, req.(*TransactionReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_PageList_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PageListRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).PageList(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/PageList",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).PageList(ctx, req.(*PageListRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_GetAmount_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AmountRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).GetAmount(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/GetAmount",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).GetAmount(ctx, req.(*AmountRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_GetDappList_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DappListReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).GetDappList(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/GetDappList",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).GetDappList(ctx, req.(*DappListReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_GetAllOpenAmount_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(OpenAmountReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).GetAllOpenAmount(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/GetAllOpenAmount",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).GetAllOpenAmount(ctx, req.(*OpenAmountReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_GetNonce_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(NonceReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).GetNonce(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/GetNonce",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).GetNonce(ctx, req.(*NonceReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_GetDappListPageList_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DappPageListReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).GetDappListPageList(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/GetDappListPageList",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).GetDappListPageList(ctx, req.(*DappPageListReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_PageListAsset_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PageListAssetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).PageListAsset(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/PageListAsset",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).PageListAsset(ctx, req.(*PageListAssetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_GetBalance_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AssetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).GetBalance(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/GetBalance",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).GetBalance(ctx, req.(*AssetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_ClientPageListNftAssetGroup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PageListNftAssetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).ClientPageListNftAssetGroup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/ClientPageListNftAssetGroup",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).ClientPageListNftAssetGroup(ctx, req.(*PageListNftAssetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_ClientPageListNftAsset_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PageListNftAssetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).ClientPageListNftAsset(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/ClientPageListNftAsset",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).ClientPageListNftAsset(ctx, req.(*PageListNftAssetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_GetNftBalance_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(NftAssetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).GetNftBalance(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/GetNftBalance",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).GetNftBalance(ctx, req.(*NftAssetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_PageListStatistic_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PageListStatisticRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).PageListStatistic(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/PageListStatistic",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).PageListStatistic(ctx, req.(*PageListStatisticRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_StatisticFundAmount_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StatisticFundRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).StatisticFundAmount(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/StatisticFundAmount",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).StatisticFundAmount(ctx, req.(*StatisticFundRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_StatisticFundRate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StatisticFundRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).StatisticFundRate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/StatisticFundRate",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).StatisticFundRate(ctx, req.(*StatisticFundRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_GetUnspentTx_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UnspentReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).GetUnspentTx(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/GetUnspentTx",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).GetUnspentTx(ctx, req.(*UnspentReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_GetNftRecord_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(NftRecordReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).GetNftRecord(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/GetNftRecord",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).GetNftRecord(ctx, req.(*NftRecordReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Transaction_JsonRpc_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JsonReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServer).JsonRpc(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.transaction.v1.Transaction/JsonRpc",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServer).JsonRpc(ctx, req.(*JsonReq))
	}
	return interceptor(ctx, in, info, handler)
}

// Transaction_ServiceDesc is the grpc.ServiceDesc for Transaction service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Transaction_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "api.transaction.v1.Transaction",
	HandlerType: (*TransactionServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateRecordFromWallet",
			Handler:    _Transaction_CreateRecordFromWallet_Handler,
		},
		{
			MethodName: "PageList",
			Handler:    _Transaction_PageList_Handler,
		},
		{
			MethodName: "GetAmount",
			Handler:    _Transaction_GetAmount_Handler,
		},
		{
			MethodName: "GetDappList",
			Handler:    _Transaction_GetDappList_Handler,
		},
		{
			MethodName: "GetAllOpenAmount",
			Handler:    _Transaction_GetAllOpenAmount_Handler,
		},
		{
			MethodName: "GetNonce",
			Handler:    _Transaction_GetNonce_Handler,
		},
		{
			MethodName: "GetDappListPageList",
			Handler:    _Transaction_GetDappListPageList_Handler,
		},
		{
			MethodName: "PageListAsset",
			Handler:    _Transaction_PageListAsset_Handler,
		},
		{
			MethodName: "GetBalance",
			Handler:    _Transaction_GetBalance_Handler,
		},
		{
			MethodName: "ClientPageListNftAssetGroup",
			Handler:    _Transaction_ClientPageListNftAssetGroup_Handler,
		},
		{
			MethodName: "ClientPageListNftAsset",
			Handler:    _Transaction_ClientPageListNftAsset_Handler,
		},
		{
			MethodName: "GetNftBalance",
			Handler:    _Transaction_GetNftBalance_Handler,
		},
		{
			MethodName: "PageListStatistic",
			Handler:    _Transaction_PageListStatistic_Handler,
		},
		{
			MethodName: "StatisticFundAmount",
			Handler:    _Transaction_StatisticFundAmount_Handler,
		},
		{
			MethodName: "StatisticFundRate",
			Handler:    _Transaction_StatisticFundRate_Handler,
		},
		{
			MethodName: "GetUnspentTx",
			Handler:    _Transaction_GetUnspentTx_Handler,
		},
		{
			MethodName: "GetNftRecord",
			Handler:    _Transaction_GetNftRecord_Handler,
		},
		{
			MethodName: "JsonRpc",
			Handler:    _Transaction_JsonRpc_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "api/transaction/v1/transaction.proto",
}
