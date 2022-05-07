package server

import (
	"context"
	"log"

	"github.com/sberss/cas3/internal/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) GetObject(ctx context.Context, req *proto.GetObjectRequest) (*proto.GetObjectResponse, error) {
	object, err := s.store.Get(req.Etag)
	if err != nil {
		log.Println(err)
		return nil, status.Error(codes.Internal, "error getting object")
	}

	return &proto.GetObjectResponse{
		Object: object,
	}, nil
}

func (s *Server) PutObject(ctx context.Context, req *proto.PutObjectRequest) (*proto.PutObjectResponse, error) {
	etag, err := s.store.Put(req.Object)
	if err != nil {
		log.Println(err)
		return nil, status.Error(codes.Internal, "error storing object")
	}

	return &proto.PutObjectResponse{
		Etag: etag,
	}, nil
}
