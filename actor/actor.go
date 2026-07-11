package actor

type Actor interface {
	OnStart(ctx *ActorContext)
	OnStop(ctx *ActorContext)
}

type Props struct {
	Actor    Actor
	Behavior Behavior
}

func NewProps(actor Actor, behavior Behavior) *Props {
	return &Props{
		Actor:    actor,
		Behavior: behavior,
	}
}
