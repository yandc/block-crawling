package service

import (
	common "block-crawling/api/common/v1"
	v1 "block-crawling/api/helloworld/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"context"
	"github.com/go-kratos/kratos/v2/log"
	"google.golang.org/protobuf/types/known/anypb"
	"strconv"
	"time"
)

// GreeterService is a greeter service.
type GreeterService struct {
	v1.UnimplementedGreeterServer

	uc *biz.GreeterUsecase
}

// NewGreeterService new a greeter service.
func NewGreeterService(uc *biz.GreeterUsecase) *GreeterService {
	return &GreeterService{uc: uc}
}

// SayHello implements helloworld.GreeterServer.
func (s *GreeterService) GetUser(ctx context.Context, in *v1.UserRequest) (*v1.UserResponse, error) {
	greeterJson, _ := data.RedisClient.Get(strconv.FormatInt(in.Id, 10)).Result()
	if greeterJson != "" {
		log.Infof("GetUser greeterJson: %v", greeterJson)
	} else {
		data.RedisClient.Set(strconv.FormatInt(in.Id, 10), "1234567890", 60*time.Second)
	}
	g, err := s.uc.FindByID(ctx, in.Id)

	if err != nil || g == nil {
		return nil, err
	}
	respose := &v1.UserResponse{Id: g.Id, Username: g.Username, Password: g.Password,
		Remark: g.Remark, CreateTime: g.CreateTime, UpdateTime: g.UpdateTime}
	return respose, err
	/*var apiResponse = &common.ApiResponse{Code:200}
	if err != nil {
		apiResponse.Status = false
		apiResponse.Msg = fmt.Sprintf("%s", err)
	} else {
		if g != nil {
			var data *anypb.Any
			data, err = s.ConvertUserResponse(g)
			if err != nil {
				apiResponse.Status = false
				apiResponse.Msg = fmt.Sprintf("%s", err)
			} else {
				apiResponse.Status = true
				apiResponse.Msg = "ok"
				apiResponse.Data = data
			}
		} else {
			apiResponse.Status = true
			apiResponse.Msg = "ok"
		}
	}
	return apiResponse, err*/
}

// CreateHello implements helloworld.GreeterServer.
func (s *GreeterService) AddUser(ctx context.Context, in *v1.UserRequest) (*common.Int64Response, error) {
	greeter := &data.Greeter{Id: in.Id, Username: in.Username, Password: in.Password,
		Remark: in.Remark, CreateTime: in.CreateTime, UpdateTime: in.UpdateTime}
	g, err := s.uc.CreateGreeter(ctx, greeter)

	if err != nil {
		return nil, err
	}
	response := &common.Int64Response{Response: g}
	return response, err
	/*var apiResponse = &common.ApiResponse{Code:200}
	if err != nil && g > 0 {
		apiResponse.Status = false
		apiResponse.Msg = fmt.Sprintf("%s", err)
	} else {
		apiResponse.Status = true
		apiResponse.Msg = "ok"
	}
	return apiResponse, err*/
}

// CreateHello implements helloworld.GreeterServer.
func (s *GreeterService) BatchAddOrUpdateUser(ctx context.Context, in *v1.UserRequests) (*common.Int64Response, error) {
	var greeters []*data.Greeter
	userRequests := in.UserRequests
	for _, userRequest := range userRequests {
		greeter := &data.Greeter{Id: userRequest.Id, Username: userRequest.Username, Password: userRequest.Password,
			Remark: userRequest.Remark, CreateTime: userRequest.CreateTime, UpdateTime: userRequest.UpdateTime}
		greeters = append(greeters, greeter)
	}
	g, err := s.uc.CreateGreeters(ctx, greeters)

	if err != nil {
		return nil, err
	}
	response := &common.Int64Response{Response: g}
	return response, err
	/*var apiResponse = &common.ApiResponse{Code:200}
	if err != nil && g > 0 {
		apiResponse.Status = false
		apiResponse.Msg = fmt.Sprintf("%s", err)
	} else {
		apiResponse.Status = true
		apiResponse.Msg = "ok"
	}
	return apiResponse, err*/
}

func (s *GreeterService) ConvertUserResponse(g *data.Greeter) (*anypb.Any, error) {
	helloRespose := &v1.UserResponse{Id: g.Id, Username: g.Username, Password: g.Password,
		Remark: g.Remark, CreateTime: g.CreateTime, UpdateTime: g.UpdateTime}
	data, err := anypb.New(helloRespose)
	return data, err
}
