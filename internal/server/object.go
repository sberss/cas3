package server

import (
	"errors"
	"io"
	"log"

	"github.com/sberss/cas3/internal/proto"
	"github.com/sberss/cas3/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) GetObject(req *proto.GetObjectRequest, stream proto.Store_GetObjectServer) error {
	etags := s.store.StartGet(req.Etag)
	for _, etag := range etags {
		objectChunk, err := s.store.GetChunk(etag)
		if err != nil {
			log.Print(err)
			return status.Error(codes.Internal, "Internal error.")
		}
		stream.Send(&proto.GetObjectResponse{
			ObjectChunk: objectChunk,
		})
	}

	return nil
}

func (s *Server) PutObject(stream proto.Store_PutObjectServer) error {
	etags := make([]string, 0)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&proto.PutObjectResponse{
				Etag: s.store.FinishPut(etags),
			})
		}
		if err != nil {
			return err
		}

		etag, err := s.store.PutChunk(req.ObjectChunk)
		if errors.Is(err, &store.ExceededChunkSizeError{}) {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if err != nil {
			log.Print(err)
			return status.Error(codes.Internal, "Internal error.")
		}

		etags = append(etags, etag)
	}

	return nil
}
