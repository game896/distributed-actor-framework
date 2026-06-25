package remote

import (
	"context"
	"fmt"
	"log"
	"net"

	"actor-framework/actor"

	"google.golang.org/grpc"
)

type Server struct {
	UnimplementedActorServiceServer
	system *actor.ActorSystem
}

func NewServer(system *actor.ActorSystem) *Server {
	return &Server{system: system}
}

func (s *Server) SendMessage(ctx context.Context, env *Envelope) (*Ack, error) {
	s.system.Send(actor.Message{
		Sender:   actor.PID(env.SenderId),
		Receiver: actor.PID(env.ReceiverId),
		Type:     env.Type,
		Payload:  env.Payload,
	})
	return &Ack{Success: true}, nil
}

func (s *Server) Start(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	grpcServer := grpc.NewServer()
	RegisterActorServiceServer(grpcServer, s)

	log.Printf("[Remote] gRPC server listening on :%d", port)
	return grpcServer.Serve(lis)
}
