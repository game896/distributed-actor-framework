package actor

type ActorContext struct {
	self    PID
	system  *ActorSystem
	process *actorProcess
}

func newActorContext(self PID, system *ActorSystem, process *actorProcess) *ActorContext {
	return &ActorContext{
		self:    self,
		system:  system,
		process: process,
	}
}

func (c *ActorContext) Send(target PID, msg Message) {
	msg.Sender = c.self
	msg.Receiver = target
	c.system.send(target, &msg)
}

func (c *ActorContext) Become(behavior Behavior) {
	c.process.setBehavior(behavior)
}

func (c *ActorContext) Self() PID {
	return c.self
}

func (c *ActorContext) System() *ActorSystem {
	return c.system
}

func (c *ActorContext) Stop(target PID) {
	c.system.stop(target)
}
