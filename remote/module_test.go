package remote

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anomalyco/distributed-actor-framework/actor"
	"github.com/anomalyco/distributed-actor-framework/internal"
	pb "github.com/anomalyco/distributed-actor-framework/remote/proto/generated"
	"google.golang.org/protobuf/proto"
)

func init() {
	internal.RegisterType("actor.Empty", func() proto.Message {
		return &pb.Empty{}
	})
}

func findFreePort() int {
	return 0
}

type remoteTestActor struct {
	mu       sync.Mutex
	messages []actor.Message
	started  bool
	stopped  bool
	ch       chan struct{}
}

func (a *remoteTestActor) OnStart(ctx *actor.ActorContext) {
	a.mu.Lock()
	a.started = true
	a.mu.Unlock()
}

func (a *remoteTestActor) OnStop(ctx *actor.ActorContext) {
	a.mu.Lock()
	a.stopped = true
	a.mu.Unlock()
}

func remoteTestBehavior(ctx *actor.ActorContext, msg actor.Message) {
	pid := ctx.Self()
	sys := ctx.System()

	val := getTestActorFromSystem(sys, pid)
	if val == nil {
		return
	}

	val.mu.Lock()
	val.messages = append(val.messages, msg)
	count := len(val.messages)
	val.mu.Unlock()

	if count >= 1 {
		select {
		case val.ch <- struct{}{}:
		default:
		}
	}
}

func getTestActorFromSystem(s *actor.ActorSystem, pid actor.PID) *remoteTestActor {
	// Use reflection-like access via the spawned actor's props isn't easily accessible
	// Instead, we'll use a global map for test tracking
	testActorsMu.RLock()
	defer testActorsMu.RUnlock()
	return testActors[pid.ID]
}

var (
	testActors   = make(map[string]*remoteTestActor)
	testActorsMu sync.RWMutex
)

func registerTestActor(id string, a *remoteTestActor) {
	testActorsMu.Lock()
	testActors[id] = a
	testActorsMu.Unlock()
}

func TestRemoteSend(t *testing.T) {
	addr1 := fmt.Sprintf("127.0.0.1:%d", 0)
	addr2 := fmt.Sprintf("127.0.0.1:%d", 0)

	sys1 := actor.New("system1")
	_, err := StartModule(sys1, addr1)
	if err != nil {
		t.Fatalf("failed to start module on system1: %v", err)
	}

	sys2 := actor.New("system2")
	mod2, err := StartModule(sys2, addr2)
	if err != nil {
		t.Fatalf("failed to start module on system2: %v", err)
	}

	actor2 := &remoteTestActor{
		ch: make(chan struct{}, 1),
	}
	registerTestActor("remote-receiver", actor2)

	pid2, err := sys2.Spawn(actor.NewProps(actor2, remoteTestBehavior), "remote-receiver")
	if err != nil {
		t.Fatalf("failed to spawn receiver: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	remotePID := actor.NewRemotePID(pid2.ID, mod2.Address())
	payload := &pb.Empty{}

	sys1.Send(remotePID, actor.NewMessage(actor.PID{}, remotePID, "remote-test", payload))

	select {
	case <-actor2.ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for remote message")
	}

	actor2.mu.Lock()
	if len(actor2.messages) != 1 {
		actor2.mu.Unlock()
		t.Fatalf("expected 1 message, got %d", len(actor2.messages))
	}
	if actor2.messages[0].Type != "remote-test" {
		t.Fatalf("expected message type 'remote-test', got '%s'", actor2.messages[0].Type)
	}
	actor2.mu.Unlock()

	mod2.Stop()
}

func TestRemoteRoundTrip(t *testing.T) {
	sys1 := actor.New("system1")
	mod1, err := StartModule(sys1, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start module on system1: %v", err)
	}

	sys2 := actor.New("system2")
	mod2, err := StartModule(sys2, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start module on system2: %v", err)
	}

	actor1 := &remoteTestActor{ch: make(chan struct{}, 1)}
	registerTestActor("remote-sender", actor1)
	pid1, err := sys1.Spawn(actor.NewProps(actor1, remoteTestBehavior), "remote-sender")
	if err != nil {
		t.Fatalf("failed to spawn sender on system1: %v", err)
	}

	actor2 := &remoteTestActor{ch: make(chan struct{}, 1)}
	registerTestActor("remote-receiver2", actor2)
	pid2, err := sys2.Spawn(actor.NewProps(actor2, remoteTestBehavior), "remote-receiver2")
	if err != nil {
		t.Fatalf("failed to spawn receiver on system2: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	remotePID1 := actor.NewRemotePID(pid2.ID, mod2.Address())
	sys1.Send(remotePID1, actor.NewMessage(pid1, remotePID1, "ping", &pb.Empty{}))

	select {
	case <-actor2.ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ping")
	}

	actor2.mu.Lock()
	if len(actor2.messages) != 1 {
		actor2.mu.Unlock()
		t.Fatalf("expected 1 message on system2, got %d", len(actor2.messages))
	}
	msg := actor2.messages[0]
	actor2.mu.Unlock()

	if msg.Type != "ping" {
		t.Fatalf("expected 'ping', got '%s'", msg.Type)
	}
	if msg.Sender.ID != "remote-sender" {
		t.Fatalf("expected sender 'remote-sender', got '%s'", msg.Sender.ID)
	}

	remotePID2 := actor.NewRemotePID(pid1.ID, mod1.Address())
	sys2.Send(remotePID2, actor.NewMessage(pid2, remotePID2, "pong", &pb.Empty{}))

	select {
	case <-actor1.ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for pong")
	}

	actor1.mu.Lock()
	if len(actor1.messages) != 1 {
		actor1.mu.Unlock()
		t.Fatalf("expected 1 message on system1, got %d", len(actor1.messages))
	}
	msg2 := actor1.messages[0]
	actor1.mu.Unlock()

	if msg2.Type != "pong" {
		t.Fatalf("expected 'pong', got '%s'", msg2.Type)
	}

	mod1.Stop()
	mod2.Stop()
}
