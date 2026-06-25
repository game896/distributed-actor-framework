package demo

import (
	"encoding/json"
	"log"

	"actor-framework/actor"
)

type ResultCollectorActor struct {
	results []TaskResult
}

func NewResultCollectorActor(ctx *actor.Context, msg actor.Message) {
	c := &ResultCollectorActor{}

	ctx.Become(func(ctx *actor.Context, msg actor.Message) {
		if msg.Type == MsgTypeResult {
			var result TaskResult
			json.Unmarshal(msg.Payload, &result)
			c.results = append(c.results, result)
			log.Printf("[Collector] result: task=%s output=%s", result.TaskID, result.Output)
		}
	})

	log.Printf("[Collector] started")
}
