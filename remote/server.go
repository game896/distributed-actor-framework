package remote

import (
	"context"
	"fmt"
	"net"

	"github.com/anomalyco/distributed-actor-framework/actor"
	"github.com/anomalyco/distributed-actor-framework/internal"
	pb "github.com/anomalyco/distributed-actor-framework/remote/proto/generated"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedActorServiceServer
	system *actor.ActorSystem
}

func (s *server) SendMessage(ctx context.Context, env *pb.MessageEnvelope) (*pb.Empty, error) {
	msg, err := internal.UnmarshalMessage(env)
	if err != nil {
		fmt.Printf("remote: failed to unmarshal message: %v\n", err)
		return &pb.Empty{}, nil
	}

	s.system.Send(msg.Receiver, *msg)
	return &pb.Empty{}, nil
}

func (s *server) Ping(ctx context.Context, _ *pb.Empty) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

type Module struct {
	system  *actor.ActorSystem
	server  *grpc.Server
	clients *clientPool
	address string
}

func StartModule(system *actor.ActorSystem, address string) (*Module, error) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("remote: failed to listen on %s: %w", address, err)
	}

	grpcServer := grpc.NewServer()

	m := &Module{
		system:  system,
		server:  grpcServer,
		clients: newClientPool(),
		address: lis.Addr().String(),
	}

	pb.RegisterActorServiceServer(grpcServer, &server{system: system})

	system.SetRemote(m, lis.Addr().String())

	go func() {
		fmt.Printf("remote: gRPC server listening on %s\n", lis.Addr().String())
		if err := grpcServer.Serve(lis); err != nil {
			fmt.Printf("remote: gRPC server stopped: %v\n", err)
		}
	}()

	return m, nil
}

func (m *Module) Send(target actor.PID, msg *actor.Message) error {
	env, err := internal.MarshalMessage(msg)
	if err != nil {
		return fmt.Errorf("remote: failed to marshal message: %w", err)
	}

	conn, err := m.clients.getConnection(target.Address)
	if err != nil {
		return fmt.Errorf("remote: failed to connect to %s: %w", target.Address, err)
	}

	client := pb.NewActorServiceClient(conn)
	_, err = client.SendMessage(context.Background(), env)
	if err != nil {
		return fmt.Errorf("remote: failed to send message to %s: %w", target.Address, err)
	}

	return nil
}

func (m *Module) Stop() {
	m.server.GracefulStop()
	m.clients.closeAll()
}

func (m *Module) Address() string {
	return m.address
}
