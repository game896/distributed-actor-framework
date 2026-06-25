package actor

type Context struct {
	Self   PID
	System *ActorSystem
}

func (c *Context) Send(to PID, msgType string, payload []byte) {
	c.System.Send(Message{
		Sender:   c.Self,
		Receiver: to,
		Type:     msgType,
		Payload:  payload,
	})
}

func (c *Context) Spawn(id PID, fn ReceiveFunc) *Actor {
	return c.System.Spawn(id, fn)
}

func (c *Context) Become(fn ReceiveFunc) {
	a := c.System.LocalActor(c.Self)
	if a != nil {
		a.Become(fn)
	}
}

func (c *Context) ActorOf(id PID) *Actor {
	return c.System.LocalActor(id)
}
