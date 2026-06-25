package remote

import (
	"context"
	"log"
	"sync"
	"time"

	"actor-framework/actor"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func init() {
	actor.SetRemoteSender(sendRemote)
}

var (
	clients   = make(map[string]*grpcClient)
	clientsMu sync.RWMutex
)

type grpcClient struct {
	addr    string
	conn    *grpc.ClientConn
	service ActorServiceClient
	mu      sync.Mutex
	ready   bool
	readyCh chan struct{}
}

func sendRemote(addr string, msg actor.Message) {
	c := getOrCreateClient(addr)
	<-c.readyCh
	c.send(msg)
}

func getOrCreateClient(addr string) *grpcClient {
	clientsMu.RLock()
	c, ok := clients[addr]
	clientsMu.RUnlock()
	if ok {
		return c
	}

	clientsMu.Lock()
	defer clientsMu.Unlock()

	if c, ok := clients[addr]; ok {
		return c
	}

	c = &grpcClient{addr: addr, readyCh: make(chan struct{})}
	clients[addr] = c
	go c.connectLoop()
	return c
}

func (c *grpcClient) connectLoop() {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := grpc.DialContext(ctx, c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()

		if err == nil {
			c.mu.Lock()
			c.conn = conn
			c.service = NewActorServiceClient(conn)
			c.ready = true
			close(c.readyCh)
			c.mu.Unlock()
			log.Printf("[Remote] connected to %s", c.addr)
			return
		}

		log.Printf("[Remote] waiting for %s...", c.addr)
		time.Sleep(1 * time.Second)
	}
}

func (c *grpcClient) send(msg actor.Message) {
	c.mu.Lock()
	client := c.service
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.SendMessage(ctx, &Envelope{
		SenderId:   string(msg.Sender),
		ReceiverId: string(msg.Receiver),
		Type:       msg.Type,
		Payload:    msg.Payload,
	})
	if err != nil {
		log.Printf("[Remote] send error to %s: %v", c.addr, err)
	}
}
