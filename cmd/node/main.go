package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/anomalyco/distributed-actor-framework/actor"
	"github.com/anomalyco/distributed-actor-framework/example/actors"
	"github.com/anomalyco/distributed-actor-framework/example/taskpb"
	"github.com/anomalyco/distributed-actor-framework/internal"
	"github.com/anomalyco/distributed-actor-framework/remote"
	"google.golang.org/protobuf/proto"
)

func init() {
	internal.RegisterType("taskpb.TaskPayload", func() proto.Message { return &taskpb.TaskPayload{} })
	internal.RegisterType("taskpb.ResultPayload", func() proto.Message { return &taskpb.ResultPayload{} })
	internal.RegisterType("taskpb.RegisterPayload", func() proto.Message { return &taskpb.RegisterPayload{} })
	internal.RegisterType("taskpb.HeartbeatPayload", func() proto.Message { return &taskpb.HeartbeatPayload{} })
}

func main() {
	mode := flag.String("mode", "", "node mode: master, worker, or client")
	addr := flag.String("addr", "0.0.0.0:0", "address this node listens on (host:port)")
	masterAddr := flag.String("master", "", "master address (host:port) for workers and client")
	workers := flag.Int("workers", 1, "number of workers to spawn (worker mode)")
	tasks := flag.Int("tasks", 5, "number of tasks to send (client mode)")
	logDir := flag.String("log-dir", "logs", "directory to write log files (additionally to terminal)")
	flag.Parse()

	if *mode == "" {
		fmt.Println("Usage: go run cmd/node/main.go --mode=master|worker|client --addr=host:port --master=host:port --workers=N --tasks=N [--log-dir=logs]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *logDir != "" {
		if err := os.MkdirAll(*logDir, 0755); err != nil {
			log.Fatalf("failed to create log directory %s: %v", *logDir, err)
		}
		logPath := filepath.Join(*logDir, fmt.Sprintf("node-%s.log", *mode))
		f, err := os.Create(logPath)
		if err != nil {
			log.Fatalf("failed to create log file %s: %v", logPath, err)
		}
		defer f.Close()

		origStdout := os.Stdout
		origStderr := os.Stderr

		rOut, wOut, _ := os.Pipe()
		rErr, wErr, _ := os.Pipe()

		os.Stdout = wOut
		os.Stderr = wErr

		go func() {
			io.Copy(io.MultiWriter(origStdout, f), rOut)
		}()
		go func() {
			io.Copy(io.MultiWriter(origStderr, f), rErr)
		}()

		defer func() {
			wOut.Close()
			wErr.Close()
			time.Sleep(50 * time.Millisecond)
			os.Stdout = origStdout
			os.Stderr = origStderr
		}()

		fmt.Printf("[Node] logging to %s (terminal + file)\n", logPath)
	}

	switch *mode {
	case "master":
		runMaster(*addr, *tasks)
	case "worker":
		if *masterAddr == "" {
			log.Fatal("--master flag is required for worker mode")
		}
		runWorker(*addr, *masterAddr, *workers)
	case "client":
		if *masterAddr == "" {
			log.Fatal("--master flag is required for client mode")
		}
		runClient(*masterAddr, *tasks)
	default:
		log.Fatalf("unknown mode: %s (use master, worker, or client)", *mode)
	}
}

func runMaster(addr string, expectedTasks int) {
	fmt.Printf("[Node] starting MASTER on %s, expecting %d tasks\n", addr, expectedTasks)

	sys := actor.New("MasterSystem")
	mod, err := remote.StartModule(sys, addr)
	if err != nil {
		log.Fatalf("failed to start remote: %v", err)
	}
	fmt.Printf("[Node] master listening on %s\n", mod.Address())

	collector := actors.NewResultCollectorActor(expectedTasks)
	collectorPID, err := sys.Spawn(actor.NewProps(collector, nil), "collector")
	if err != nil {
		log.Fatalf("failed to spawn collector: %v", err)
	}

	master := actors.NewMasterActor(collectorPID)
	_, err = sys.Spawn(actor.NewProps(master, nil), "master")
	if err != nil {
		log.Fatalf("failed to spawn master: %v", err)
	}

	fmt.Printf("[Node] master ready, waiting for workers and tasks...\n")
	fmt.Println()

	select {
	case <-collector.WaitDone():
	case <-time.After(60 * time.Second):
		fmt.Println("[Node] timed out waiting for results")
	}

	fmt.Println()
	fmt.Println("[Node] shutting down master...")
	sys.Shutdown()
	mod.Stop()
	fmt.Println("[Node] done.")
}

func runWorker(addr string, masterAddr string, numWorkers int) {
	fmt.Printf("[Node] starting WORKER on %s, connecting to master at %s\n", addr, masterAddr)

	sys := actor.New("WorkerSystem")
	mod, err := remote.StartModule(sys, addr)
	if err != nil {
		log.Fatalf("failed to start remote: %v", err)
	}
	fmt.Printf("[Node] worker listening on %s\n", mod.Address())

	masterPID := actor.NewRemotePID("master", masterAddr)
	for i := 0; i < numWorkers; i++ {
		id := fmt.Sprintf("worker-%d@%s", i+1, addr)
		w := actors.NewWorkerActor(id, masterPID)
		_, err := sys.Spawn(actor.NewProps(w, nil), id)
		if err != nil {
			log.Fatalf("failed to spawn %s: %v", id, err)
		}
		fmt.Printf("[Node] spawned %s\n", id)
	}

	fmt.Printf("[Node] %d worker(s) running. Press Ctrl+C to stop.\n", numWorkers)
	select {}
}

func runClient(masterAddr string, numTasks int) {
	fmt.Printf("[Node] starting CLIENT, submitting %d tasks to master at %s\n", numTasks, masterAddr)

	sys := actor.New("ClientSystem")
	mod, err := remote.StartModule(sys, "0.0.0.0:0")
	if err != nil {
		log.Fatalf("failed to create remote client: %v", err)
	}
	defer mod.Stop()
	defer sys.Shutdown()

	time.Sleep(100 * time.Millisecond)

	masterPID := actor.NewRemotePID("master", masterAddr)
	fmt.Println()
	fmt.Println("=== Submitting Tasks ===")
	fmt.Println()

	for i := 0; i < numTasks; i++ {
		task := createTask(i)
		sys.Send(masterPID, actor.NewMessage(actor.PID{}, masterPID, actors.MsgTypeTask, task))
		fmt.Printf("  Submitted task %s (%s)\n", task.TaskId, task.TaskType)
		time.Sleep(10 * time.Millisecond)
	}

	fmt.Println()
	fmt.Printf("[Node] all %d tasks submitted. Check master terminal for results.\n", numTasks)
}

func createTask(i int) *taskpb.TaskPayload {
	taskID := fmt.Sprintf("task-%03d", i+1)

	switch i % 4 {
	case 0:
		return &taskpb.TaskPayload{
			TaskId: taskID, TaskType: "sum",
			A: int32(i * 10), B: int32(i * 5),
		}
	case 1:
		return &taskpb.TaskPayload{
			TaskId: taskID, TaskType: "factorial",
			N: int32((i % 7) + 3),
		}
	case 2:
		inputs := []string{"hello", "world", "golang", "actor", "framework", "remote", "gRPC", "distributed"}
		return &taskpb.TaskPayload{
			TaskId: taskID, TaskType: "reverse",
			Input: inputs[i%len(inputs)],
		}
	default:
		return &taskpb.TaskPayload{
			TaskId: taskID, TaskType: "sleep",
			DurationMs: int32(((i % 5) + 1) * 100),
		}
	}
}
