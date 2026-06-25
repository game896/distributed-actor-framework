package actor

type PID string

type Message struct {
	Sender   PID
	Receiver PID
	Type     string
	Payload  []byte
}

type ReceiveFunc func(ctx *Context, msg Message)
