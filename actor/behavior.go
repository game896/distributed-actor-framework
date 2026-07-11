package actor

type Behavior func(ctx *ActorContext, msg Message)
