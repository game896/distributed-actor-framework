package main

import (
	"fmt"
	"log"
	"time"

	"github.com/anomalyco/distributed-actor-framework/actor"
	"github.com/anomalyco/distributed-actor-framework/example/actors"
	"github.com/anomalyco/distributed-actor-framework/example/taskpb"
	"github.com/anomalyco/distributed-actor-framework/internal"
	"github.com/anomalyco/distributed-actor-framework/remote"
	"google.golang.org/protobuf/proto"
)

func init() {
	internal.RegisterType("taskpb.TaskPayload", func() proto.Message {
		return &taskpb.TaskPayload{}
	})
	internal.RegisterType("taskpb.ResultPayload", func() proto.Message {
		return &taskpb.ResultPayload{}
	})
	internal.RegisterType("taskpb.RegisterPayload", func() proto.Message {
		return &taskpb.RegisterPayload{}
	})
	internal.RegisterType("taskpb.HeartbeatPayload", func() proto.Message {
		return &taskpb.HeartbeatPayload{}
	})
}

func main() {
	fmt.Println("=== Distributed Task Processing Demo ===")
	fmt.Println()

	numTasks := 10

	sysMaster := actor.New("MasterSystem")
	masterMod, err := remote.StartModule(sysMaster, "127.0.0.1:0")
	if err != nil {
		log.Fatalf("failed to start remote on master: %v", err)
	}
	fmt.Printf("Master system listening on %s\n", masterMod.Address())

	collector := actors.NewResultCollectorActor(numTasks)
	collectorPID, err := sysMaster.Spawn(actor.NewProps(collector, nil), "collector")
	if err != nil {
		log.Fatalf("failed to spawn collector: %v", err)
	}

	master := actors.NewMasterActor(collectorPID)
	_, err = sysMaster.Spawn(actor.NewProps(master, nil), "master")
	if err != nil {
		log.Fatalf("failed to spawn master: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	sysWorker := actor.New("WorkerSystem")
	workerMod, err := remote.StartModule(sysWorker, "127.0.0.1:0")
	if err != nil {
		log.Fatalf("failed to start remote on worker: %v", err)
	}
	fmt.Printf("Worker system listening on %s\n", workerMod.Address())

	workerMasterPID := actor.NewRemotePID("master", masterMod.Address())

	numWorkers := 3
	for i := 0; i < numWorkers; i++ {
		id := fmt.Sprintf("worker-%d@%s", i+1, workerMod.Address())
		w := actors.NewWorkerActor(id, workerMasterPID)
		_, err := sysWorker.Spawn(actor.NewProps(w, nil), id)
		if err != nil {
			log.Fatalf("failed to spawn %s: %v", id, err)
		}
		fmt.Printf("Spawned %s on worker system\n", id)
	}

	time.Sleep(200 * time.Millisecond)

	fmt.Println()
	fmt.Println("=== Dispatching Tasks ===")
	fmt.Println()

	for i := 0; i < numTasks; i++ {
		task := createTask(i)
		taskPID := actor.NewRemotePID("master", masterMod.Address())
		sysWorker.Send(taskPID, actor.NewMessage(actor.PID{}, taskPID, actors.MsgTypeTask, task))
		time.Sleep(5 * time.Millisecond)
	}

	fmt.Println()
	fmt.Println("=== Waiting for results ===")
	fmt.Println()

	select {
	case <-collector.WaitDone():
	case <-time.After(15 * time.Second):
		fmt.Println("Timed out waiting for results")
	}

	fmt.Println()
	fmt.Println("Shutting down...")
	sysWorker.Shutdown()
	workerMod.Stop()
	sysMaster.Shutdown()
	masterMod.Stop()
	fmt.Println("Done.")
}

func createTask(i int) *taskpb.TaskPayload {
	taskID := fmt.Sprintf("task-%03d", i+1)

	switch i % 4 {
	case 0:
		a := i * 10
		b := i * 5
		return &taskpb.TaskPayload{
			TaskId:   taskID,
			TaskType: "sum",
			A:        int32(a),
			B:        int32(b),
		}
	case 1:
		n := (i % 7) + 3
		return &taskpb.TaskPayload{
			TaskId:   taskID,
			TaskType: "factorial",
			N:        int32(n),
		}
	case 2:
		inputs := []string{"hello", "world", "golang", "actor", "framework", "remote", "gRPC", "distributed"}
		input := inputs[i%len(inputs)]
		return &taskpb.TaskPayload{
			TaskId:   taskID,
			TaskType: "reverse",
			Input:    input,
		}
	default:
		ms := ((i % 5) + 1) * 100
		return &taskpb.TaskPayload{
			TaskId:     taskID,
			TaskType:   "sleep",
			DurationMs: int32(ms),
		}
	}
}
