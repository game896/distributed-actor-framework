package actors

import (
	"fmt"
	"sync"

	"github.com/anomalyco/distributed-actor-framework/actor"
	"github.com/anomalyco/distributed-actor-framework/example/taskpb"
)

type ResultCollectorActor struct {
	results    []*taskpb.ResultPayload
	expected   int
	done       int
	doneCh     chan struct{}
	mu         sync.Mutex
}

func NewResultCollectorActor(expected int) *ResultCollectorActor {
	return &ResultCollectorActor{
		expected: expected,
		doneCh:   make(chan struct{}, 1),
	}
}

func (c *ResultCollectorActor) OnStart(ctx *actor.ActorContext) {
	ctx.Become(c.collectorBehavior)
}

func (c *ResultCollectorActor) OnStop(ctx *actor.ActorContext) {
	fmt.Println("[Collector] shutdown")
}

func (c *ResultCollectorActor) WaitDone() <-chan struct{} {
	return c.doneCh
}

func (c *ResultCollectorActor) collectorBehavior(ctx *actor.ActorContext, msg actor.Message) {
	if msg.Type != MsgTypeResult {
		return
	}

	result := msg.Payload.(*taskpb.ResultPayload)

	c.mu.Lock()
	c.results = append(c.results, result)
	c.done++
	completed := c.done
	total := c.expected
	c.mu.Unlock()

	fmt.Printf("[Collector] received result for task %s (%d/%d)\n", result.TaskId, completed, total)

	if completed >= total {
		fmt.Println("\n========== ALL TASKS COMPLETE ==========")
		fmt.Println("Results:")
		for i, r := range c.results {
			status := "OK"
			if !r.Success {
				status = "FAIL"
			}
			fmt.Printf("  %d. [%s] task=%s type=%s worker=%s result=%s\n",
				i+1, status, r.TaskId, r.TaskType, r.WorkerId, r.Result)
		}
		fmt.Println("========================================")
		c.doneCh <- struct{}{}
	}
}
