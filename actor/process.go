package actor

import "sync"

type actorProcess struct {
	pid      PID
	props    *Props
	mailbox  *Mailbox
	behavior Behavior
	context  *ActorContext
	system   *ActorSystem
	stopCh   chan struct{}
	wg       sync.WaitGroup
	mu       sync.RWMutex
	running  bool
}

func newActorProcess(system *ActorSystem, pid PID, props *Props) *actorProcess {
	if props.Behavior == nil {
		props.Behavior = func(ctx *ActorContext, msg Message) {}
	}

	p := &actorProcess{
		pid:      pid,
		props:    props,
		mailbox:  NewMailbox(DefaultMailboxSize),
		behavior: props.Behavior,
		system:   system,
		stopCh:   make(chan struct{}),
	}

	p.context = newActorContext(pid, system, p)
	return p
}

func (p *actorProcess) start() {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return
	}
	p.running = true
	p.mu.Unlock()

	p.props.Actor.OnStart(p.context)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer p.props.Actor.OnStop(p.context)

		for {
			select {
			case msg, ok := <-p.mailbox.Pop():
				if !ok {
					return
				}
				p.behavior(p.context, *msg)
			case <-p.stopCh:
				return
			}
		}
	}()
}

func (p *actorProcess) stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	p.mu.Unlock()

	close(p.stopCh)
	p.wg.Wait()
}

func (p *actorProcess) send(msg *Message) error {
	return p.mailbox.Push(msg)
}

func (p *actorProcess) setBehavior(behavior Behavior) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.behavior = behavior
}
