package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"

	"actor-framework/actor"
	"actor-framework/demo"
	"actor-framework/remote"
)

func main() {
	args := os.Args[1:]
	if len(args) < 2 {
		log.Fatal("Usage: worker <listen_port> <master_addr> [worker_id]")
	}

	port, _ := strconv.Atoi(args[0])
	masterAddr := args[1]
	workerID := "worker"
	if len(args) > 2 {
		workerID = args[2]
	}

	addr := "localhost:" + args[0]

	system := actor.NewActorSystem()

	system.RegisterRemote("master", masterAddr)

	info, _ := json.Marshal(demo.RegisterInfo{PID: "master", Addr: addr})

	workerPID := actor.PID(workerID)
	system.Spawn(workerPID, func(ctx *actor.Context, msg actor.Message) {
		demo.NewWorkerActor(ctx, actor.Message{Payload: info})
	})

	remoteServer := remote.NewServer(system)
	log.Fatal(remoteServer.Start(port))
}
