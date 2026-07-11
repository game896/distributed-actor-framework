package actors

import (
	"context"
	"fmt"
	"time"

	"github.com/anomalyco/distributed-actor-framework/actor"
	"github.com/anomalyco/distributed-actor-framework/example/taskpb"
	pb "github.com/anomalyco/distributed-actor-framework/remote/proto/generated"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type WorkerActor struct {
	workerID      string
	masterPID     actor.PID
	pendingTask   *taskpb.TaskPayload
	pendingSender actor.PID
	stopCh        chan struct{}
}

func NewWorkerActor(id string, masterPID actor.PID) *WorkerActor {
	return &WorkerActor{
		workerID:  id,
		masterPID: masterPID,
	}
}

func (w *WorkerActor) OnStart(ctx *actor.ActorContext) {
	w.stopCh = make(chan struct{})
	ctx.Become(w.idleBehavior)

	go func() {
		for {
			if w.pingMaster() {
				break
			}
			fmt.Printf("[Worker %s] master not ready, retrying in 2s...\n", w.workerID)
			select {
			case <-w.stopCh:
				return
			case <-time.After(2 * time.Second):
			}
		}

		payload := &taskpb.RegisterPayload{
			WorkerId: w.workerID,
			Address:  ctx.Self().Address,
		}
		ctx.Send(w.masterPID, actor.NewMessage(ctx.Self(), w.masterPID, MsgTypeRegister, payload))
		fmt.Printf("[Worker %s] registered with master\n", w.workerID)

		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		fails := 0
		for {
			select {
			case <-ticker.C:
				if !w.pingMaster() {
					fails++
					if fails >= 3 {
						fmt.Printf("[Worker %s] master unreachable, stopping heartbeat\n", w.workerID)
						return
					}
					continue
				}
				fails = 0
				hb := &taskpb.HeartbeatPayload{
					WorkerId: w.workerID,
					Address:  ctx.Self().Address,
				}
				ctx.Send(w.masterPID, actor.NewMessage(ctx.Self(), w.masterPID, MsgTypeHeartbeat, hb))
			case <-w.stopCh:
				return
			}
		}
	}()

	fmt.Printf("[Worker %s] started, waiting for master...\n", w.workerID)
}

func (w *WorkerActor) OnStop(ctx *actor.ActorContext) {
	close(w.stopCh)
	fmt.Printf("[Worker %s] stopped\n", w.workerID)
}

func (w *WorkerActor) pingMaster() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(w.masterPID.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false
	}
	defer conn.Close()

	client := pb.NewActorServiceClient(conn)
	_, err = client.Ping(ctx, &pb.Empty{})
	return err == nil
}

func (w *WorkerActor) idleBehavior(ctx *actor.ActorContext, msg actor.Message) {
	if msg.Type != MsgTypeTask {
		return
	}

	task := msg.Payload.(*taskpb.TaskPayload)
	fmt.Printf("[Worker %s] IDLE -> accepted task %s (%s)\n", w.workerID, task.TaskId, task.TaskType)

	w.pendingTask = task
	w.pendingSender = msg.Sender

	ctx.Become(w.busyBehavior)
	ctx.Send(ctx.Self(), actor.NewMessage(ctx.Self(), ctx.Self(), "process", nil))
}

func (w *WorkerActor) busyBehavior(ctx *actor.ActorContext, msg actor.Message) {
	switch msg.Type {
	case MsgTypeTask:
		task := msg.Payload.(*taskpb.TaskPayload)
		fmt.Printf("[Worker %s] BUSY -> rejected task %s\n", w.workerID, task.TaskId)
		result := &taskpb.ResultPayload{
			TaskId:   task.TaskId,
			TaskType: task.TaskType,
			Result:   "rejected: worker busy",
			WorkerId: w.workerID,
			Success:  false,
		}
		ctx.Send(msg.Sender, actor.NewMessage(ctx.Self(), msg.Sender, MsgTypeResult, result))

	case "process":
		result := w.executeTask(w.pendingTask)
		result.WorkerId = w.workerID

		fmt.Printf("[Worker %s] BUSY -> task %s done, result=%s\n",
			w.workerID, w.pendingTask.TaskId, result.Result)

		ctx.Send(w.pendingSender, actor.NewMessage(ctx.Self(), w.pendingSender, MsgTypeResult, result))

		w.pendingTask = nil
		ctx.Become(w.idleBehavior)
		fmt.Printf("[Worker %s] becoming IDLE\n", w.workerID)
	}
}

func (w *WorkerActor) executeTask(task *taskpb.TaskPayload) *taskpb.ResultPayload {
	var result string
	var success bool

	switch task.TaskType {
	case "sum":
		result = fmt.Sprintf("%d", task.A+task.B)
		success = true
	case "factorial":
		r := int64(1)
		for i := int32(2); i <= task.N; i++ {
			r *= int64(i)
		}
		result = fmt.Sprintf("%d", r)
		success = true
	case "reverse":
		runes := []rune(task.Input)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		result = string(runes)
		success = true
	case "sleep":
		time.Sleep(time.Duration(task.DurationMs) * time.Millisecond)
		result = fmt.Sprintf("slept for %dms", task.DurationMs)
		success = true
	default:
		result = fmt.Sprintf("unknown task type: %s", task.TaskType)
		success = false
	}

	return &taskpb.ResultPayload{
		TaskId:   task.TaskId,
		TaskType: task.TaskType,
		Result:   result,
		Success:  success,
	}
}
