package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	"actor-framework/actor"
	"actor-framework/demo"
	"actor-framework/remote"
)

func main() {
	args := os.Args[1:]
	if len(args) < 1 {
		log.Fatal("Usage: master <listen_port> [worker1_addr worker2_addr ...]")
	}

	port, _ := strconv.Atoi(args[0])

	system := actor.NewActorSystem()

	collector := system.Spawn("collector", demo.NewResultCollectorActor)
	system.Spawn("master", func(ctx *actor.Context, msg actor.Message) {
		demo.NewMasterActor(ctx, actor.Message{Payload: []byte(collector.PID)})
	})

	remoteServer := remote.NewServer(system)
	go remoteServer.Start(port)

	time.Sleep(1 * time.Second)

	tasks := []demo.Task{
		{ID: "1", Op: "sum", A: 10, B: 20},
		{ID: "2", Op: "factorial", A: 10},
		{ID: "3", Op: "reverse", Input: "hello"},
		{ID: "4", Op: "sleep", SleepMs: 500},
		{ID: "5", Op: "sum", A: 100, B: 200},
	}

	for _, task := range tasks {
		data, _ := json.Marshal(task)
		system.Send(actor.Message{
			Sender:   "main",
			Receiver: "master",
			Type:     demo.MsgTypeTask,
			Payload:  data,
		})
		time.Sleep(200 * time.Millisecond)
	}

	log.Printf("[Main] all tasks submitted, waiting for workers...")
	select {}
}
