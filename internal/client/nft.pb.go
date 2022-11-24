// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v3.15.8
// source: api/nft/v1/nft.proto

package v1

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type GetNftInfoRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Chain   string                       `protobuf:"bytes,1,opt,name=chain,proto3" json:"chain,omitempty"`
	NftInfo []*GetNftInfoRequest_NftInfo `protobuf:"bytes,2,rep,name=nftInfo,proto3" json:"nftInfo,omitempty"`
}

func (x *GetNftInfoRequest) Reset() {
	*x = GetNftInfoRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_nft_v1_nft_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetNftInfoRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetNftInfoRequest) ProtoMessage() {}

func (x *GetNftInfoRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_nft_v1_nft_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetNftInfoRequest.ProtoReflect.Descriptor instead.
func (*GetNftInfoRequest) Descriptor() ([]byte, []int) {
	return file_api_nft_v1_nft_proto_rawDescGZIP(), []int{0}
}

func (x *GetNftInfoRequest) GetChain() string {
	if x != nil {
		return x.Chain
	}
	return ""
}

func (x *GetNftInfoRequest) GetNftInfo() []*GetNftInfoRequest_NftInfo {
	if x != nil {
		return x.NftInfo
	}
	return nil
}

type GetNftReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ok     bool                       `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
	ErrMsg string                     `protobuf:"bytes,2,opt,name=errMsg,proto3" json:"errMsg,omitempty"`
	Data   []*GetNftReply_NftInfoResp `protobuf:"bytes,3,rep,name=data,proto3" json:"data,omitempty"`
}

func (x *GetNftReply) Reset() {
	*x = GetNftReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_nft_v1_nft_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetNftReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetNftReply) ProtoMessage() {}

func (x *GetNftReply) ProtoReflect() protoreflect.Message {
	mi := &file_api_nft_v1_nft_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetNftReply.ProtoReflect.Descriptor instead.
func (*GetNftReply) Descriptor() ([]byte, []int) {
	return file_api_nft_v1_nft_proto_rawDescGZIP(), []int{1}
}

func (x *GetNftReply) GetOk() bool {
	if x != nil {
		return x.Ok
	}
	return false
}

func (x *GetNftReply) GetErrMsg() string {
	if x != nil {
		return x.ErrMsg
	}
	return ""
}

func (x *GetNftReply) GetData() []*GetNftReply_NftInfoResp {
	if x != nil {
		return x.Data
	}
	return nil
}

type GetNftCollectionInfoReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Chain   string `protobuf:"bytes,1,opt,name=chain,proto3" json:"chain,omitempty"`
	Address string `protobuf:"bytes,2,opt,name=address,proto3" json:"address,omitempty"`
}

func (x *GetNftCollectionInfoReq) Reset() {
	*x = GetNftCollectionInfoReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_nft_v1_nft_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetNftCollectionInfoReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetNftCollectionInfoReq) ProtoMessage() {}

func (x *GetNftCollectionInfoReq) ProtoReflect() protoreflect.Message {
	mi := &file_api_nft_v1_nft_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetNftCollectionInfoReq.ProtoReflect.Descriptor instead.
func (*GetNftCollectionInfoReq) Descriptor() ([]byte, []int) {
	return file_api_nft_v1_nft_proto_rawDescGZIP(), []int{2}
}

func (x *GetNftCollectionInfoReq) GetChain() string {
	if x != nil {
		return x.Chain
	}
	return ""
}

func (x *GetNftCollectionInfoReq) GetAddress() string {
	if x != nil {
		return x.Address
	}
	return ""
}

type GetNftCollectionInfoReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ok     bool                            `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
	ErrMsg string                          `protobuf:"bytes,2,opt,name=errMsg,proto3" json:"errMsg,omitempty"`
	Data   *GetNftCollectionInfoReply_Data `protobuf:"bytes,3,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *GetNftCollectionInfoReply) Reset() {
	*x = GetNftCollectionInfoReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_nft_v1_nft_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetNftCollectionInfoReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetNftCollectionInfoReply) ProtoMessage() {}

func (x *GetNftCollectionInfoReply) ProtoReflect() protoreflect.Message {
	mi := &file_api_nft_v1_nft_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetNftCollectionInfoReply.ProtoReflect.Descriptor instead.
func (*GetNftCollectionInfoReply) Descriptor() ([]byte, []int) {
	return file_api_nft_v1_nft_proto_rawDescGZIP(), []int{3}
}

func (x *GetNftCollectionInfoReply) GetOk() bool {
	if x != nil {
		return x.Ok
	}
	return false
}

func (x *GetNftCollectionInfoReply) GetErrMsg() string {
	if x != nil {
		return x.ErrMsg
	}
	return ""
}

func (x *GetNftCollectionInfoReply) GetData() *GetNftCollectionInfoReply_Data {
	if x != nil {
		return x.Data
	}
	return nil
}

type GetNftInfoRequest_NftInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TokenId      string `protobuf:"bytes,1,opt,name=tokenId,proto3" json:"tokenId,omitempty"`
	TokenAddress string `protobuf:"bytes,2,opt,name=tokenAddress,proto3" json:"tokenAddress,omitempty"`
}

func (x *GetNftInfoRequest_NftInfo) Reset() {
	*x = GetNftInfoRequest_NftInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_nft_v1_nft_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetNftInfoRequest_NftInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetNftInfoRequest_NftInfo) ProtoMessage() {}

func (x *GetNftInfoRequest_NftInfo) ProtoReflect() protoreflect.Message {
	mi := &file_api_nft_v1_nft_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetNftInfoRequest_NftInfo.ProtoReflect.Descriptor instead.
func (*GetNftInfoRequest_NftInfo) Descriptor() ([]byte, []int) {
	return file_api_nft_v1_nft_proto_rawDescGZIP(), []int{0, 0}
}

func (x *GetNftInfoRequest_NftInfo) GetTokenId() string {
	if x != nil {
		return x.TokenId
	}
	return ""
}

func (x *GetNftInfoRequest_NftInfo) GetTokenAddress() string {
	if x != nil {
		return x.TokenAddress
	}
	return ""
}

type GetNftReply_NftInfoResp struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TokenId               string `protobuf:"bytes,1,opt,name=tokenId,proto3" json:"tokenId,omitempty"`
	TokenAddress          string `protobuf:"bytes,2,opt,name=tokenAddress,proto3" json:"tokenAddress,omitempty"`
	Name                  string `protobuf:"bytes,3,opt,name=name,proto3" json:"name,omitempty"`
	Symbol                string `protobuf:"bytes,4,opt,name=symbol,proto3" json:"symbol,omitempty"`
	TokenType             string `protobuf:"bytes,5,opt,name=tokenType,proto3" json:"tokenType,omitempty"`
	ImageURL              string `protobuf:"bytes,6,opt,name=imageURL,proto3" json:"imageURL,omitempty"`
	ImageOriginalURL      string `protobuf:"bytes,7,opt,name=imageOriginalURL,proto3" json:"imageOriginalURL,omitempty"`
	Description           string `protobuf:"bytes,8,opt,name=description,proto3" json:"description,omitempty"`
	Chain                 string `protobuf:"bytes,9,opt,name=chain,proto3" json:"chain,omitempty"`
	CollectionName        string `protobuf:"bytes,10,opt,name=collectionName,proto3" json:"collectionName,omitempty"`
	CollectionSlug        string `protobuf:"bytes,11,opt,name=collectionSlug,proto3" json:"collectionSlug,omitempty"`
	Rarity                string `protobuf:"bytes,12,opt,name=rarity,proto3" json:"rarity,omitempty"`
	Network               string `protobuf:"bytes,13,opt,name=network,proto3" json:"network,omitempty"`
	Properties            string `protobuf:"bytes,14,opt,name=properties,proto3" json:"properties,omitempty"`
	CollectionDescription string `protobuf:"bytes,15,opt,name=collectionDescription,proto3" json:"collectionDescription,omitempty"`
	NftName               string `protobuf:"bytes,16,opt,name=nftName,proto3" json:"nftName,omitempty"`
	CollectionImageURL    string `protobuf:"bytes,17,opt,name=collectionImageURL,proto3" json:"collectionImageURL,omitempty"`
	AnimationURL          string `protobuf:"bytes,18,opt,name=animationURL,proto3" json:"animationURL,omitempty"`
}

func (x *GetNftReply_NftInfoResp) Reset() {
	*x = GetNftReply_NftInfoResp{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_nft_v1_nft_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetNftReply_NftInfoResp) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetNftReply_NftInfoResp) ProtoMessage() {}

func (x *GetNftReply_NftInfoResp) ProtoReflect() protoreflect.Message {
	mi := &file_api_nft_v1_nft_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetNftReply_NftInfoResp.ProtoReflect.Descriptor instead.
func (*GetNftReply_NftInfoResp) Descriptor() ([]byte, []int) {
	return file_api_nft_v1_nft_proto_rawDescGZIP(), []int{1, 0}
}

func (x *GetNftReply_NftInfoResp) GetTokenId() string {
	if x != nil {
		return x.TokenId
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetTokenAddress() string {
	if x != nil {
		return x.TokenAddress
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetSymbol() string {
	if x != nil {
		return x.Symbol
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetTokenType() string {
	if x != nil {
		return x.TokenType
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetImageURL() string {
	if x != nil {
		return x.ImageURL
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetImageOriginalURL() string {
	if x != nil {
		return x.ImageOriginalURL
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetChain() string {
	if x != nil {
		return x.Chain
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetCollectionName() string {
	if x != nil {
		return x.CollectionName
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetCollectionSlug() string {
	if x != nil {
		return x.CollectionSlug
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetRarity() string {
	if x != nil {
		return x.Rarity
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetNetwork() string {
	if x != nil {
		return x.Network
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetProperties() string {
	if x != nil {
		return x.Properties
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetCollectionDescription() string {
	if x != nil {
		return x.CollectionDescription
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetNftName() string {
	if x != nil {
		return x.NftName
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetCollectionImageURL() string {
	if x != nil {
		return x.CollectionImageURL
	}
	return ""
}

func (x *GetNftReply_NftInfoResp) GetAnimationURL() string {
	if x != nil {
		return x.AnimationURL
	}
	return ""
}

type GetNftCollectionInfoReply_Data struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Chain       string `protobuf:"bytes,1,opt,name=chain,proto3" json:"chain,omitempty"`
	Address     string `protobuf:"bytes,2,opt,name=address,proto3" json:"address,omitempty"`
	Name        string `protobuf:"bytes,3,opt,name=name,proto3" json:"name,omitempty"`
	Slug        string `protobuf:"bytes,4,opt,name=slug,proto3" json:"slug,omitempty"`
	ImageURL    string `protobuf:"bytes,5,opt,name=imageURL,proto3" json:"imageURL,omitempty"`
	Description string `protobuf:"bytes,6,opt,name=description,proto3" json:"description,omitempty"`
	TokenType   string `protobuf:"bytes,7,opt,name=tokenType,proto3" json:"tokenType,omitempty"`
}

func (x *GetNftCollectionInfoReply_Data) Reset() {
	*x = GetNftCollectionInfoReply_Data{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_nft_v1_nft_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetNftCollectionInfoReply_Data) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetNftCollectionInfoReply_Data) ProtoMessage() {}

func (x *GetNftCollectionInfoReply_Data) ProtoReflect() protoreflect.Message {
	mi := &file_api_nft_v1_nft_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetNftCollectionInfoReply_Data.ProtoReflect.Descriptor instead.
func (*GetNftCollectionInfoReply_Data) Descriptor() ([]byte, []int) {
	return file_api_nft_v1_nft_proto_rawDescGZIP(), []int{3, 0}
}

func (x *GetNftCollectionInfoReply_Data) GetChain() string {
	if x != nil {
		return x.Chain
	}
	return ""
}

func (x *GetNftCollectionInfoReply_Data) GetAddress() string {
	if x != nil {
		return x.Address
	}
	return ""
}

func (x *GetNftCollectionInfoReply_Data) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *GetNftCollectionInfoReply_Data) GetSlug() string {
	if x != nil {
		return x.Slug
	}
	return ""
}

func (x *GetNftCollectionInfoReply_Data) GetImageURL() string {
	if x != nil {
		return x.ImageURL
	}
	return ""
}

func (x *GetNftCollectionInfoReply_Data) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *GetNftCollectionInfoReply_Data) GetTokenType() string {
	if x != nil {
		return x.TokenType
	}
	return ""
}

var File_api_nft_v1_nft_proto protoreflect.FileDescriptor

var file_api_nft_v1_nft_proto_rawDesc = []byte{
	0x0a, 0x14, 0x61, 0x70, 0x69, 0x2f, 0x6e, 0x66, 0x74, 0x2f, 0x76, 0x31, 0x2f, 0x6e, 0x66, 0x74,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0a, 0x61, 0x70, 0x69, 0x2e, 0x6e, 0x66, 0x74, 0x2e,
	0x76, 0x31, 0x22, 0xb3, 0x01, 0x0a, 0x11, 0x47, 0x65, 0x74, 0x4e, 0x66, 0x74, 0x49, 0x6e, 0x66,
	0x6f, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x63, 0x68, 0x61, 0x69,
	0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x63, 0x68, 0x61, 0x69, 0x6e, 0x12, 0x3f,
	0x0a, 0x07, 0x6e, 0x66, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32,
	0x25, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6e, 0x66, 0x74, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74,
	0x4e, 0x66, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x4e,
	0x66, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x07, 0x6e, 0x66, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x1a,
	0x47, 0x0a, 0x07, 0x4e, 0x66, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x18, 0x0a, 0x07, 0x74, 0x6f,
	0x6b, 0x65, 0x6e, 0x49, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x74, 0x6f, 0x6b,
	0x65, 0x6e, 0x49, 0x64, 0x12, 0x22, 0x0a, 0x0c, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x41, 0x64, 0x64,
	0x72, 0x65, 0x73, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x74, 0x6f, 0x6b, 0x65,
	0x6e, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x22, 0xcc, 0x05, 0x0a, 0x0b, 0x47, 0x65, 0x74,
	0x4e, 0x66, 0x74, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x0e, 0x0a, 0x02, 0x6f, 0x6b, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x08, 0x52, 0x02, 0x6f, 0x6b, 0x12, 0x16, 0x0a, 0x06, 0x65, 0x72, 0x72, 0x4d,
	0x73, 0x67, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x65, 0x72, 0x72, 0x4d, 0x73, 0x67,
	0x12, 0x37, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x23,
	0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6e, 0x66, 0x74, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x4e,
	0x66, 0x74, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x2e, 0x4e, 0x66, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x52,
	0x65, 0x73, 0x70, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x1a, 0xdb, 0x04, 0x0a, 0x0b, 0x4e, 0x66,
	0x74, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x65, 0x73, 0x70, 0x12, 0x18, 0x0a, 0x07, 0x74, 0x6f, 0x6b,
	0x65, 0x6e, 0x49, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x74, 0x6f, 0x6b, 0x65,
	0x6e, 0x49, 0x64, 0x12, 0x22, 0x0a, 0x0c, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x41, 0x64, 0x64, 0x72,
	0x65, 0x73, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x74, 0x6f, 0x6b, 0x65, 0x6e,
	0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x73,
	0x79, 0x6d, 0x62, 0x6f, 0x6c, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x79, 0x6d,
	0x62, 0x6f, 0x6c, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x54, 0x79, 0x70, 0x65,
	0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x54, 0x79, 0x70,
	0x65, 0x12, 0x1a, 0x0a, 0x08, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x55, 0x52, 0x4c, 0x18, 0x06, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x08, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x55, 0x52, 0x4c, 0x12, 0x2a, 0x0a,
	0x10, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x4f, 0x72, 0x69, 0x67, 0x69, 0x6e, 0x61, 0x6c, 0x55, 0x52,
	0x4c, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x10, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x4f, 0x72,
	0x69, 0x67, 0x69, 0x6e, 0x61, 0x6c, 0x55, 0x52, 0x4c, 0x12, 0x20, 0x0a, 0x0b, 0x64, 0x65, 0x73,
	0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x08, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b,
	0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x14, 0x0a, 0x05, 0x63,
	0x68, 0x61, 0x69, 0x6e, 0x18, 0x09, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x63, 0x68, 0x61, 0x69,
	0x6e, 0x12, 0x26, 0x0a, 0x0e, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x4e,
	0x61, 0x6d, 0x65, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0e, 0x63, 0x6f, 0x6c, 0x6c, 0x65,
	0x63, 0x74, 0x69, 0x6f, 0x6e, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x26, 0x0a, 0x0e, 0x63, 0x6f, 0x6c,
	0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x53, 0x6c, 0x75, 0x67, 0x18, 0x0b, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0e, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x53, 0x6c, 0x75,
	0x67, 0x12, 0x16, 0x0a, 0x06, 0x72, 0x61, 0x72, 0x69, 0x74, 0x79, 0x18, 0x0c, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x06, 0x72, 0x61, 0x72, 0x69, 0x74, 0x79, 0x12, 0x18, 0x0a, 0x07, 0x6e, 0x65, 0x74,
	0x77, 0x6f, 0x72, 0x6b, 0x18, 0x0d, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6e, 0x65, 0x74, 0x77,
	0x6f, 0x72, 0x6b, 0x12, 0x1e, 0x0a, 0x0a, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65,
	0x73, 0x18, 0x0e, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74,
	0x69, 0x65, 0x73, 0x12, 0x34, 0x0a, 0x15, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f,
	0x6e, 0x44, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x0f, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x15, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x44, 0x65,
	0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x18, 0x0a, 0x07, 0x6e, 0x66, 0x74,
	0x4e, 0x61, 0x6d, 0x65, 0x18, 0x10, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6e, 0x66, 0x74, 0x4e,
	0x61, 0x6d, 0x65, 0x12, 0x2e, 0x0a, 0x12, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f,
	0x6e, 0x49, 0x6d, 0x61, 0x67, 0x65, 0x55, 0x52, 0x4c, 0x18, 0x11, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x12, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x6d, 0x61, 0x67, 0x65,
	0x55, 0x52, 0x4c, 0x12, 0x22, 0x0a, 0x0c, 0x61, 0x6e, 0x69, 0x6d, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x55, 0x52, 0x4c, 0x18, 0x12, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x61, 0x6e, 0x69, 0x6d, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x55, 0x52, 0x4c, 0x22, 0x49, 0x0a, 0x17, 0x47, 0x65, 0x74, 0x4e, 0x66,
	0x74, 0x43, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x6e, 0x66, 0x6f, 0x52,
	0x65, 0x71, 0x12, 0x14, 0x0a, 0x05, 0x63, 0x68, 0x61, 0x69, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x05, 0x63, 0x68, 0x61, 0x69, 0x6e, 0x12, 0x18, 0x0a, 0x07, 0x61, 0x64, 0x64, 0x72,
	0x65, 0x73, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x61, 0x64, 0x64, 0x72, 0x65,
	0x73, 0x73, 0x22, 0xc0, 0x02, 0x0a, 0x19, 0x47, 0x65, 0x74, 0x4e, 0x66, 0x74, 0x43, 0x6f, 0x6c,
	0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x65, 0x70, 0x6c, 0x79,
	0x12, 0x0e, 0x0a, 0x02, 0x6f, 0x6b, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x02, 0x6f, 0x6b,
	0x12, 0x16, 0x0a, 0x06, 0x65, 0x72, 0x72, 0x4d, 0x73, 0x67, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x06, 0x65, 0x72, 0x72, 0x4d, 0x73, 0x67, 0x12, 0x3e, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x2a, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6e, 0x66, 0x74,
	0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x4e, 0x66, 0x74, 0x43, 0x6f, 0x6c, 0x6c, 0x65, 0x63,
	0x74, 0x69, 0x6f, 0x6e, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x2e, 0x44, 0x61,
	0x74, 0x61, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x1a, 0xba, 0x01, 0x0a, 0x04, 0x44, 0x61, 0x74,
	0x61, 0x12, 0x14, 0x0a, 0x05, 0x63, 0x68, 0x61, 0x69, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x05, 0x63, 0x68, 0x61, 0x69, 0x6e, 0x12, 0x18, 0x0a, 0x07, 0x61, 0x64, 0x64, 0x72, 0x65,
	0x73, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73,
	0x73, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x73, 0x6c, 0x75, 0x67, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x04, 0x73, 0x6c, 0x75, 0x67, 0x12, 0x1a, 0x0a, 0x08, 0x69, 0x6d, 0x61,
	0x67, 0x65, 0x55, 0x52, 0x4c, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x69, 0x6d, 0x61,
	0x67, 0x65, 0x55, 0x52, 0x4c, 0x12, 0x20, 0x0a, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70,
	0x74, 0x69, 0x6f, 0x6e, 0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x64, 0x65, 0x73, 0x63,
	0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x6f, 0x6b, 0x65, 0x6e,
	0x54, 0x79, 0x70, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x74, 0x6f, 0x6b, 0x65,
	0x6e, 0x54, 0x79, 0x70, 0x65, 0x32, 0xaf, 0x01, 0x0a, 0x03, 0x4e, 0x66, 0x74, 0x12, 0x44, 0x0a,
	0x0a, 0x47, 0x65, 0x74, 0x4e, 0x66, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x1d, 0x2e, 0x61, 0x70,
	0x69, 0x2e, 0x6e, 0x66, 0x74, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x4e, 0x66, 0x74, 0x49,
	0x6e, 0x66, 0x6f, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x17, 0x2e, 0x61, 0x70, 0x69,
	0x2e, 0x6e, 0x66, 0x74, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x4e, 0x66, 0x74, 0x52, 0x65,
	0x70, 0x6c, 0x79, 0x12, 0x62, 0x0a, 0x14, 0x47, 0x65, 0x74, 0x4e, 0x66, 0x74, 0x43, 0x6f, 0x6c,
	0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x23, 0x2e, 0x61, 0x70,
	0x69, 0x2e, 0x6e, 0x66, 0x74, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x4e, 0x66, 0x74, 0x43,
	0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x65, 0x71,
	0x1a, 0x25, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6e, 0x66, 0x74, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65,
	0x74, 0x4e, 0x66, 0x74, 0x43, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x6e,
	0x66, 0x6f, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x42, 0x3e, 0x0a, 0x0a, 0x61, 0x70, 0x69, 0x2e, 0x6e,
	0x66, 0x74, 0x2e, 0x76, 0x31, 0x50, 0x01, 0x5a, 0x2e, 0x67, 0x69, 0x74, 0x6c, 0x61, 0x62, 0x2e,
	0x62, 0x69, 0x78, 0x69, 0x6e, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6d, 0x69, 0x6c, 0x69, 0x2f, 0x6e,
	0x6f, 0x64, 0x65, 0x2d, 0x70, 0x72, 0x6f, 0x78, 0x79, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x6e, 0x66,
	0x74, 0x2f, 0x76, 0x31, 0x3b, 0x76, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_api_nft_v1_nft_proto_rawDescOnce sync.Once
	file_api_nft_v1_nft_proto_rawDescData = file_api_nft_v1_nft_proto_rawDesc
)

func file_api_nft_v1_nft_proto_rawDescGZIP() []byte {
	file_api_nft_v1_nft_proto_rawDescOnce.Do(func() {
		file_api_nft_v1_nft_proto_rawDescData = protoimpl.X.CompressGZIP(file_api_nft_v1_nft_proto_rawDescData)
	})
	return file_api_nft_v1_nft_proto_rawDescData
}

var file_api_nft_v1_nft_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_api_nft_v1_nft_proto_goTypes = []interface{}{
	(*GetNftInfoRequest)(nil),              // 0: api.nft.v1.GetNftInfoRequest
	(*GetNftReply)(nil),                    // 1: api.nft.v1.GetNftReply
	(*GetNftCollectionInfoReq)(nil),        // 2: api.nft.v1.GetNftCollectionInfoReq
	(*GetNftCollectionInfoReply)(nil),      // 3: api.nft.v1.GetNftCollectionInfoReply
	(*GetNftInfoRequest_NftInfo)(nil),      // 4: api.nft.v1.GetNftInfoRequest.NftInfo
	(*GetNftReply_NftInfoResp)(nil),        // 5: api.nft.v1.GetNftReply.NftInfoResp
	(*GetNftCollectionInfoReply_Data)(nil), // 6: api.nft.v1.GetNftCollectionInfoReply.Data
}
var file_api_nft_v1_nft_proto_depIdxs = []int32{
	4, // 0: api.nft.v1.GetNftInfoRequest.nftInfo:type_name -> api.nft.v1.GetNftInfoRequest.NftInfo
	5, // 1: api.nft.v1.GetNftReply.data:type_name -> api.nft.v1.GetNftReply.NftInfoResp
	6, // 2: api.nft.v1.GetNftCollectionInfoReply.data:type_name -> api.nft.v1.GetNftCollectionInfoReply.Data
	0, // 3: api.nft.v1.Nft.GetNftInfo:input_type -> api.nft.v1.GetNftInfoRequest
	2, // 4: api.nft.v1.Nft.GetNftCollectionInfo:input_type -> api.nft.v1.GetNftCollectionInfoReq
	1, // 5: api.nft.v1.Nft.GetNftInfo:output_type -> api.nft.v1.GetNftReply
	3, // 6: api.nft.v1.Nft.GetNftCollectionInfo:output_type -> api.nft.v1.GetNftCollectionInfoReply
	5, // [5:7] is the sub-list for method output_type
	3, // [3:5] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_api_nft_v1_nft_proto_init() }
func file_api_nft_v1_nft_proto_init() {
	if File_api_nft_v1_nft_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_api_nft_v1_nft_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetNftInfoRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_nft_v1_nft_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetNftReply); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_nft_v1_nft_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetNftCollectionInfoReq); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_nft_v1_nft_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetNftCollectionInfoReply); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_nft_v1_nft_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetNftInfoRequest_NftInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_nft_v1_nft_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetNftReply_NftInfoResp); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_nft_v1_nft_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetNftCollectionInfoReply_Data); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_api_nft_v1_nft_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_api_nft_v1_nft_proto_goTypes,
		DependencyIndexes: file_api_nft_v1_nft_proto_depIdxs,
		MessageInfos:      file_api_nft_v1_nft_proto_msgTypes,
	}.Build()
	File_api_nft_v1_nft_proto = out.File
	file_api_nft_v1_nft_proto_rawDesc = nil
	file_api_nft_v1_nft_proto_goTypes = nil
	file_api_nft_v1_nft_proto_depIdxs = nil
}
