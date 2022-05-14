package server

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/sberss/cas3/internal/proto"
	"github.com/sberss/cas3/internal/store"
	"google.golang.org/grpc"
)

type Server struct {
	grpcServer         *grpc.Server
	grpcServerFinished chan bool

	store store.Store
}

func NewServer(storeConfig *store.Config) *Server {
	store := store.NewS3Store(storeConfig)

	server := &Server{
		store: store,
	}

	grpcServer := grpc.NewServer()
	proto.RegisterStoreServer(grpcServer, server)
	server.grpcServer = grpcServer

	return server
}

// Start starts the grpc server.
func (s *Server) Start() {
	s.grpcServerFinished = make(chan bool)
	go func() {
		log.Print("starting grpc server")
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", 8082))
		if err != nil {
			log.Fatal(err)
		}

		err = s.grpcServer.Serve(listener)
		if err != nil {
			log.Fatal("grpcServer error", err)
		}

		close(s.grpcServerFinished)
	}()
}

// Stop stops the grpc server
func (s *Server) Stop() {
	// This will close the grpcServerFinished channel when done.
	go func() {
		s.grpcServer.GracefulStop()
	}()

	didShutdown := false
	if s.grpcServerFinished != nil {
		timer := time.NewTimer(10 * time.Second)
		select {
		case <-s.grpcServerFinished:
			didShutdown = true
		case <-timer.C:
		}
		timer.Stop()
	}

	// Force shutdown if we failed to stop gracefully.
	if !didShutdown {
		log.Print("failed to shutdown grpc server gracefully, forcing shutdown")
		s.grpcServer.Stop()
	}
}
