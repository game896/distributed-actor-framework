package demo

import (
	"encoding/json"
	"log"
	"strconv"

	"actor-framework/actor"
)

const (
	MsgTypeTask     = "TASK"
	MsgTypeResult   = "RESULT"
	MsgTypeReg      = "REGISTER"
	MsgTypeStatus   = "STATUS"
	MsgTypeShutdown = "SHUTDOWN"
)

type Task struct {
	ID      string `json:"id"`
	Op      string `json:"op"`
	A       int    `json:"a,omitempty"`
	B       int    `json:"b,omitempty"`
	Input   string `json:"input,omitempty"`
	SleepMs int    `json:"sleep_ms,omitempty"`
}

type TaskResult struct {
	TaskID string `json:"task_id"`
	Output string `json:"output"`
	Error  string `json:"error,omitempty"`
}

type MasterActor struct {
	workers   []actor.PID
	collector actor.PID
	next      int
	pending   [][]byte
}

func NewMasterActor(ctx *actor.Context, msg actor.Message) {
	var collectorPID actor.PID
	if len(msg.Payload) > 0 {
		collectorPID = actor.PID(string(msg.Payload))
	}

	m := &MasterActor{collector: collectorPID}

	ctx.Become(func(ctx *actor.Context, msg actor.Message) {
		switch msg.Type {
		case MsgTypeReg:
			var info RegisterInfo
			json.Unmarshal(msg.Payload, &info)
			pid := actor.PID(info.PID)
			m.workers = append(m.workers, pid)
			ctx.ActorOf(pid) // ensure it's tracked
			ctx.System.RegisterRemote(pid, info.Addr)
			log.Printf("[Master] worker registered: %s at %s (total: %d)", pid, info.Addr, len(m.workers))

			for _, data := range m.pending {
				m.dispatch(ctx, data)
			}
			m.pending = nil

		case MsgTypeTask:
			if len(m.workers) == 0 {
				m.pending = append(m.pending, msg.Payload)
				log.Printf("[Master] queued task (no workers yet)")
				return
			}
			m.dispatch(ctx, msg.Payload)

		case MsgTypeResult:
			ctx.Send(m.collector, MsgTypeResult, msg.Payload)
		}
	})

	log.Printf("[Master] started, collector: %s", collectorPID)
}

func (m *MasterActor) dispatch(ctx *actor.Context, payload []byte) {
	worker := m.workers[m.next]
	m.next = (m.next + 1) % len(m.workers)
	ctx.Send(worker, MsgTypeTask, payload)

	var task Task
	json.Unmarshal(payload, &task)
	log.Printf("[Master] task %s sent to %s", task.ID, worker)
}

func ExecuteTask(task Task) TaskResult {
	switch task.Op {
	case "sum":
		return TaskResult{TaskID: task.ID, Output: strconv.Itoa(task.A + task.B)}
	case "factorial":
		r := 1
		for i := 2; i <= task.A; i++ {
			r *= i
		}
		return TaskResult{TaskID: task.ID, Output: strconv.Itoa(r)}
	case "reverse":
		runes := []rune(task.Input)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return TaskResult{TaskID: task.ID, Output: string(runes)}
	case "sleep":
		return TaskResult{TaskID: task.ID, Output: "done"}
	default:
		return TaskResult{TaskID: task.ID, Error: "unknown op: " + task.Op}
	}
}
