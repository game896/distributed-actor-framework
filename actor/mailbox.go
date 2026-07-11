package actor

import "fmt"

const DefaultMailboxSize = 100

type Mailbox struct {
	ch chan *Message
}

func NewMailbox(size int) *Mailbox {
	if size <= 0 {
		size = DefaultMailboxSize
	}
	return &Mailbox{
		ch: make(chan *Message, size),
	}
}

func (m *Mailbox) Push(msg *Message) error {
	select {
	case m.ch <- msg:
		return nil
	default:
		return fmt.Errorf("mailbox full")
	}
}

func (m *Mailbox) Pop() <-chan *Message {
	return m.ch
}

func (m *Mailbox) Len() int {
	return len(m.ch)
}
