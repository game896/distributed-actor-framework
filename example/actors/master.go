package actors

import (
	"fmt"
	"sync"
	"time"

	"github.com/anomalyco/distributed-actor-framework/actor"
	"github.com/anomalyco/distributed-actor-framework/example/taskpb"
)

const heartbeatTimeout = 8 * time.Second

type WorkerInfo struct {
	PID      actor.PID
	LastSeen time.Time
}

type PendingTask struct {
	Payload   *taskpb.TaskPayload
	Retries   int
	MaxRetry  int
}

type MasterActor struct {
	registry  map[string]*WorkerInfo
	order     []string
	nextIndex int
	collector actor.PID
	pending   map[string]*PendingTask
	total     int
	done      int
	mu        sync.Mutex
	stopCh    chan struct{}
}

func NewMasterActor(collectorPID actor.PID) *MasterActor {
	return &MasterActor{
		collector: collectorPID,
		registry:  make(map[string]*WorkerInfo),
		order:     make([]string, 0),
		pending:   make(map[string]*PendingTask),
	}
}

func (m *MasterActor) OnStart(ctx *actor.ActorContext) {
	m.stopCh = make(chan struct{})
	ctx.Become(m.masterBehavior)

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ctx.Send(ctx.Self(), actor.NewMessage(ctx.Self(), ctx.Self(), MsgTypeCheckHeartbeats, nil))
			case <-m.stopCh:
				return
			}
		}
	}()
}

func (m *MasterActor) OnStop(ctx *actor.ActorContext) {
	close(m.stopCh)
	fmt.Printf("[Master] shutdown - processed %d/%d tasks, %d workers remaining\n",
		m.done, m.total, len(m.registry))
}

func (m *MasterActor) registerWorker(workerID string, pid actor.PID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.registry[workerID]; !exists {
		m.order = append(m.order, workerID)
		fmt.Printf("[Master] worker order updated: %v\n", m.order)
	}
	m.registry[workerID] = &WorkerInfo{
		PID:      pid,
		LastSeen: time.Now(),
	}
}

func (m *MasterActor) getNextWorkerExcluding(excludeID string) *WorkerInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.order) == 0 {
		return nil
	}
	for attempt := 0; attempt < len(m.order); attempt++ {
		if m.nextIndex >= len(m.order) {
			m.nextIndex = 0
		}
		id := m.order[m.nextIndex]
		m.nextIndex++
		if id != excludeID {
			return m.registry[id]
		}
	}
	return nil
}

func (m *MasterActor) masterBehavior(ctx *actor.ActorContext, msg actor.Message) {
	switch msg.Type {
	case MsgTypeRegister:
		payload := msg.Payload.(*taskpb.RegisterPayload)
		pid := actor.NewRemotePID(payload.WorkerId, payload.Address)
		m.registerWorker(payload.WorkerId, pid)
		fmt.Printf("[Master] registered worker %s@%s (total: %d)\n",
			payload.WorkerId, payload.Address, len(m.registry))

	case MsgTypeHeartbeat:
		payload := msg.Payload.(*taskpb.HeartbeatPayload)
		m.mu.Lock()
		if info, ok := m.registry[payload.WorkerId]; ok {
			info.LastSeen = time.Now()
		}
		m.mu.Unlock()

	case MsgTypeCheckHeartbeats:
		now := time.Now()
		m.mu.Lock()
		for id, info := range m.registry {
			if now.Sub(info.LastSeen) > heartbeatTimeout {
				fmt.Printf("[Master] worker %s timed out (last seen %v ago), removing\n",
					id, now.Sub(info.LastSeen))
				delete(m.registry, id)
			}
		}
		newOrder := make([]string, 0, len(m.registry))
		for _, id := range m.order {
			if _, ok := m.registry[id]; ok {
				newOrder = append(newOrder, id)
			}
		}
		m.order = newOrder
		if m.nextIndex >= len(m.order) {
			m.nextIndex = 0
		}
		m.mu.Unlock()

	case MsgTypeTask:
		worker := m.getNextWorkerExcluding("")
		if worker == nil {
			fmt.Println("[Master] no workers available, dropping task")
			return
		}
		task := msg.Payload.(*taskpb.TaskPayload)

		m.mu.Lock()
		m.total++
		m.pending[task.TaskId] = &PendingTask{
			Payload:  task,
			MaxRetry: 3,
		}
		fmt.Printf("[Master] pending tasks: %d\n", len(m.pending))
		m.mu.Unlock()

		msg.Receiver = worker.PID
		ctx.Send(worker.PID, msg)
		fmt.Printf("[Master] dispatched task %s -> %s@%s\n",
			task.TaskId, worker.PID.ID, worker.PID.Address)

	case MsgTypeResult:
		result := msg.Payload.(*taskpb.ResultPayload)

		m.mu.Lock()
		pt, hasPending := m.pending[result.TaskId]
		rejectedWorker := result.WorkerId
		m.mu.Unlock()

		if !result.Success && hasPending && pt.Retries < pt.MaxRetry {
			pt.Retries++
			worker := m.getNextWorkerExcluding(rejectedWorker)
			if worker != nil {
				fmt.Printf("[Master] retrying task %s (attempt %d/%d) on %s@%s (was %s)\n",
					result.TaskId, pt.Retries, pt.MaxRetry,
					worker.PID.ID, worker.PID.Address, rejectedWorker)
				ctx.Send(worker.PID, actor.NewMessage(ctx.Self(), worker.PID, MsgTypeTask, pt.Payload))
				return
			}
			fmt.Printf("[Master] no other worker to retry task %s on\n", result.TaskId)
		}

		m.mu.Lock()
		delete(m.pending, result.TaskId)
		m.done++
		m.mu.Unlock()

		ctx.Send(m.collector, msg)
		fmt.Printf("[Master] collected result for task %s (%d/%d)\n",
			result.TaskId, m.done, m.total)
	}
}
