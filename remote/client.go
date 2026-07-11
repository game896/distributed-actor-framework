package remote

import (
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type clientPool struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
}

func newClientPool() *clientPool {
	return &clientPool{
		conns: make(map[string]*grpc.ClientConn),
	}
}

func (p *clientPool) getConnection(address string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	conn, exists := p.conns[address]
	p.mu.RUnlock()

	if exists {
		return conn, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	conn, exists = p.conns[address]
	if exists {
		return conn, nil
	}

	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}

	p.conns[address] = conn
	return conn, nil
}

func (p *clientPool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, conn := range p.conns {
		conn.Close()
		delete(p.conns, addr)
	}
}
