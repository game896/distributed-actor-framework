package actor

import (
	"fmt"
	"sync"
)

type ActorSystem struct {
	name      string
	processes sync.Map
	remote    RemoteSender
	address   string
	mu        sync.RWMutex
}

type RemoteSender interface {
	Send(target PID, msg *Message) error
}

func New(name string) *ActorSystem {
	return &ActorSystem{
		name:    name,
		address: "",
	}
}

func (s *ActorSystem) Name() string {
	return s.name
}

func (s *ActorSystem) SetRemote(remote RemoteSender, address string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.remote = remote
	s.address = address
}

func (s *ActorSystem) Spawn(props *Props, id string) (PID, error) {
	if props == nil || props.Actor == nil {
		return PID{}, fmt.Errorf("actor: props or actor is nil")
	}

	pid := PID{ID: id, Address: s.address}

	if _, loaded := s.processes.LoadOrStore(pid, nil); loaded {
		s.processes.Delete(pid)
		return PID{}, fmt.Errorf("actor: actor with id %s already exists", id)
	}

	process := newActorProcess(s, pid, props)
	s.processes.Store(pid, process)

	process.start()

	return pid, nil
}

func (s *ActorSystem) send(target PID, msg *Message) {
	if target.IsLocal() || target.Address == s.address {
		s.sendLocal(target, msg)
		return
	}

	s.mu.RLock()
	remote := s.remote
	s.mu.RUnlock()

	if remote != nil {
		if err := remote.Send(target, msg); err != nil {
			fmt.Printf("actor: failed to send remote message to %s: %v\n", target, err)
		}
		return
	}

	fmt.Printf("actor: no remote module configured, cannot send to %s\n", target)
}

func (s *ActorSystem) sendLocal(target PID, msg *Message) {
	val, ok := s.processes.Load(target)
	if !ok {
		fmt.Printf("actor: target actor %s not found locally\n", target)
		return
	}

	process, ok := val.(*actorProcess)
	if !ok {
		fmt.Printf("actor: invalid process type for %s\n", target)
		return
	}

	if err := process.send(msg); err != nil {
		fmt.Printf("actor: failed to deliver message to %s: %v\n", target, err)
	}
}

func (s *ActorSystem) Send(target PID, msg Message) {
	s.send(target, &msg)
}

func (s *ActorSystem) stop(target PID) {
	val, ok := s.processes.Load(target)
	if !ok {
		return
	}

	process, ok := val.(*actorProcess)
	if !ok {
		return
	}

	process.stop()
	s.processes.Delete(target)
}

func (s *ActorSystem) Stop(target PID) {
	s.stop(target)
}

func (s *ActorSystem) Shutdown() {
	s.processes.Range(func(key, value interface{}) bool {
		if process, ok := value.(*actorProcess); ok {
			process.stop()
		}
		s.processes.Delete(key)
		return true
	})
}
