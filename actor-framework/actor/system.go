package actor

import (
	"log"
	"sync"
)

var remoteSend func(addr string, msg Message)

func SetRemoteSender(fn func(addr string, msg Message)) {
	remoteSend = fn
}

type ActorSystem struct {
	actors map[PID]*Actor
	remote map[PID]string
	mu     sync.RWMutex
}

func NewActorSystem() *ActorSystem {
	return &ActorSystem{
		actors: make(map[PID]*Actor),
		remote: make(map[PID]string),
	}
}

func (s *ActorSystem) Spawn(id PID, fn ReceiveFunc) *Actor {
	a := &Actor{
		PID:     id,
		Mailbox: make(chan Message, 100),
		receive: fn,
		system:  s,
	}

	s.mu.Lock()
	s.actors[id] = a
	s.mu.Unlock()

	go a.start()
	log.Printf("[System] spawned actor %s", id)

	return a
}

func (s *ActorSystem) RegisterRemote(id PID, addr string) {
	s.mu.Lock()
	s.remote[id] = addr
	s.mu.Unlock()
}

func (s *ActorSystem) Send(msg Message) {
	s.mu.RLock()
	a, local := s.actors[msg.Receiver]
	addr, remote := s.remote[msg.Receiver]
	s.mu.RUnlock()

	if local {
		a.Mailbox <- msg
	} else if remote && remoteSend != nil {
		go remoteSend(addr, msg)
	} else {
		log.Printf("[System] unknown receiver: %s", msg.Receiver)
	}
}

func (s *ActorSystem) LocalActor(id PID) *Actor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.actors[id]
}

type Actor struct {
	PID     PID
	Mailbox chan Message
	receive ReceiveFunc
	system  *ActorSystem
	ctx     *Context
}

func (a *Actor) start() {
	a.ctx = &Context{Self: a.PID, System: a.system}

	if a.receive == nil {
		return
	}

	initFn := a.receive
	initFn(a.ctx, Message{})

	for msg := range a.Mailbox {
		if a.receive != nil {
			a.receive(a.ctx, msg)
		}
	}
}

func (a *Actor) Become(fn ReceiveFunc) {
	a.receive = fn
}

func (a *Actor) Stop() {
	close(a.Mailbox)
	log.Printf("[Actor] %s stopped", a.PID)
}
