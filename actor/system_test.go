package actor

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type testActor struct {
	started  bool
	stopped  bool
	mu       sync.Mutex
	messages []Message
	wg       sync.WaitGroup
}

func (a *testActor) OnStart(ctx *ActorContext) {
	a.mu.Lock()
	a.started = true
	a.mu.Unlock()
}

func (a *testActor) OnStop(ctx *ActorContext) {
	a.mu.Lock()
	a.stopped = true
	a.mu.Unlock()
}

func testBehavior(ctx *ActorContext, msg Message) {
	actor := ctx.Self()
	sys := ctx.System()

	ta := sys.getTestActor(actor)
	if ta == nil {
		return
	}

	ta.mu.Lock()
	ta.messages = append(ta.messages, msg)
	count := len(ta.messages)
	ta.mu.Unlock()

	if count >= 3 {
		ta.wg.Done()
	}
}

func (s *ActorSystem) getTestActor(pid PID) *testActor {
	val, ok := s.processes.Load(pid)
	if !ok {
		return nil
	}
	process := val.(*actorProcess)
	return process.props.Actor.(*testActor)
}

func TestSpawnAndSend(t *testing.T) {
	sys := New("test")
	defer sys.Shutdown()

	actor := &testActor{}
	actor.wg.Add(1)

	pid, err := sys.Spawn(NewProps(actor, testBehavior), "test-1")
	if err != nil {
		t.Fatalf("failed to spawn actor: %v", err)
	}

	if pid.ID != "test-1" {
		t.Fatalf("expected pid.ID test-1, got %s", pid.ID)
	}

	time.Sleep(10 * time.Millisecond)

	actor.mu.Lock()
	if !actor.started {
		actor.mu.Unlock()
		t.Fatal("actor OnStart was not called")
	}
	actor.mu.Unlock()

	sys.Send(pid, NewMessage(PID{}, pid, "msg1", nil))
	sys.Send(pid, NewMessage(PID{}, pid, "msg2", nil))
	sys.Send(pid, NewMessage(PID{}, pid, "msg3", nil))

	done := make(chan struct{})
	go func() {
		actor.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for messages")
	}

	actor.mu.Lock()
	if len(actor.messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(actor.messages))
	}
	if actor.messages[0].Type != "msg1" {
		t.Fatalf("expected msg1, got %s", actor.messages[0].Type)
	}
	actor.mu.Unlock()
}

func TestBecome(t *testing.T) {
	sys := New("test")
	defer sys.Shutdown()

	processed := make(chan string, 2)

	initialBehavior := func(ctx *ActorContext, msg Message) {
		if msg.Type == "switch" {
			ctx.Become(func(ctx2 *ActorContext, msg2 Message) {
				processed <- msg2.Type
			})
			return
		}
		processed <- msg.Type
	}

	actor := &testActor{}

	pid, err := sys.Spawn(NewProps(actor, initialBehavior), "become-test")
	if err != nil {
		t.Fatalf("failed to spawn: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	sys.Send(pid, NewMessage(PID{}, pid, "before", nil))
	sys.Send(pid, NewMessage(PID{}, pid, "switch", nil))
	sys.Send(pid, NewMessage(PID{}, pid, "after", nil))

	got := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case m := <-processed:
			got = append(got, m)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for message %d, got %v", i, got)
		}
	}

	if got[0] != "before" {
		t.Fatalf("expected first message 'before' (old behavior), got '%s'", got[0])
	}
	if got[1] != "after" {
		t.Fatalf("expected second message 'after' (new behavior), got '%s'", got[1])
	}
}

func TestLifecycle(t *testing.T) {
	sys := New("test")

	actor := &testActor{}
	pid, err := sys.Spawn(NewProps(actor, testBehavior), "lifecycle-test")
	if err != nil {
		t.Fatalf("failed to spawn: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	actor.mu.Lock()
	if !actor.started {
		actor.mu.Unlock()
		t.Fatal("OnStart not called")
	}
	actor.mu.Unlock()

	sys.Stop(pid)

	time.Sleep(50 * time.Millisecond)

	actor.mu.Lock()
	if !actor.stopped {
		actor.mu.Unlock()
		t.Fatal("OnStop not called after Stop()")
	}
	actor.mu.Unlock()
}

func TestStopNonExistent(t *testing.T) {
	sys := New("test")
	defer sys.Shutdown()

	pid := NewPID("does-not-exist")
	sys.Stop(pid)
}

func TestSpawnDuplicateID(t *testing.T) {
	sys := New("test")
	defer sys.Shutdown()

	actor := &testActor{}
	_, err := sys.Spawn(NewProps(actor, testBehavior), "dup-id")
	if err != nil {
		t.Fatalf("first spawn failed: %v", err)
	}

	_, err = sys.Spawn(NewProps(&testActor{}, testBehavior), "dup-id")
	if err == nil {
		t.Fatal("expected error for duplicate id")
	}
}

func TestShutdown(t *testing.T) {
	sys := New("test")

	actors := make([]*testActor, 5)
	for i := 0; i < 5; i++ {
		actor := &testActor{}
		actors[i] = actor
		id := fmt.Sprintf("shutdown-actor-%d", i)
		pid, err := sys.Spawn(NewProps(actor, testBehavior), id)
		if err != nil {
			t.Fatalf("failed to spawn actor %d: %v", i, err)
		}
		_ = pid
	}

	time.Sleep(10 * time.Millisecond)

	sys.Shutdown()

	time.Sleep(50 * time.Millisecond)

	for i, a := range actors {
		a.mu.Lock()
		if !a.stopped {
			a.mu.Unlock()
			t.Fatalf("actor %d OnStop not called after Shutdown", i)
		}
		a.mu.Unlock()
	}
}

func TestContextSelf(t *testing.T) {
	sys := New("test")
	defer sys.Shutdown()

	selfCheckBehavior := func(ctx *ActorContext, msg Message) {
		pid := ctx.Self()
		if pid.ID != "self-test" {
			t.Errorf("expected self ID 'self-test', got '%s'", pid.ID)
		}
		actor := ctx.System().getTestActor(pid)
		if actor == nil {
			return
		}
		actor.wg.Done()
	}

	actor := &testActor{}
	actor.wg.Add(1)

	pid, err := sys.Spawn(NewProps(actor, selfCheckBehavior), "self-test")
	if err != nil {
		t.Fatalf("failed to spawn: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	sys.Send(pid, NewMessage(PID{}, pid, "check", nil))

	done := make(chan struct{})
	go func() {
		actor.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for self check")
	}
}
