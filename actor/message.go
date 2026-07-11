package actor

type Message struct {
	Sender   PID
	Receiver PID
	Type     string
	Payload  any
}

func NewMessage(sender PID, receiver PID, msgType string, payload any) Message {
	return Message{
		Sender:   sender,
		Receiver: receiver,
		Type:     msgType,
		Payload:  payload,
	}
}
