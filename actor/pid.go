package actor

import "fmt"

type PID struct {
	ID      string
	Address string
}

func NewPID(id string) PID {
	return PID{ID: id}
}

func NewRemotePID(id string, address string) PID {
	return PID{ID: id, Address: address}
}

func (p PID) String() string {
	if p.Address == "" {
		return p.ID
	}
	return fmt.Sprintf("%s@%s", p.ID, p.Address)
}

func (p PID) IsLocal() bool {
	return p.Address == ""
}

func (p PID) GoString() string {
	return p.String()
}
