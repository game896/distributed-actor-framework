package demo

import (
	"encoding/json"
	"log"
	"time"

	"actor-framework/actor"
)

type WorkerActor struct {
	master actor.PID
	addr   string
}

type RegisterInfo struct {
	PID  string `json:"pid"`
	Addr string `json:"addr"`
}

func NewWorkerActor(ctx *actor.Context, msg actor.Message) {
	w := &WorkerActor{master: ""}

	if len(msg.Payload) > 0 {
		var info RegisterInfo
		json.Unmarshal(msg.Payload, &info)
		w.master = actor.PID(info.PID)
		w.addr = info.Addr
	}

	reg, _ := json.Marshal(RegisterInfo{PID: string(ctx.Self), Addr: w.addr})
	ctx.Send(w.master, MsgTypeReg, reg)

	ctx.Become(w.idleBehavior)

	log.Printf("[Worker] %s started, master: %s, addr: %s", ctx.Self, w.master, w.addr)
}

func (w *WorkerActor) idleBehavior(ctx *actor.Context, msg actor.Message) {
	if msg.Type == MsgTypeTask {
		var task Task
		json.Unmarshal(msg.Payload, &task)

		log.Printf("[Worker] %s received task %s (%s), switching to BUSY", ctx.Self, task.ID, task.Op)

		ctx.Become(w.busyBehavior)

		result := ExecuteTask(task)

		time.Sleep(100 * time.Millisecond)

		data, _ := json.Marshal(result)
		ctx.Send(w.master, MsgTypeResult, data)

		log.Printf("[Worker] %s completed task %s, switching back to IDLE", ctx.Self, task.ID)
		ctx.Become(w.idleBehavior)
	}
}

func (w *WorkerActor) busyBehavior(ctx *actor.Context, msg actor.Message) {
	log.Printf("[Worker] %s is BUSY, rejecting %s", ctx.Self, msg.Type)
}
