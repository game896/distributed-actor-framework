# Actor Task Framework

## Overview

This project is a lightweight implementation of an actor-based framework in Java, designed to support asynchronous message passing and distributed execution.

The framework is demonstrated through a simple distributed task processing system where tasks are executed by remote worker actors.

---

## Key Features

- Actor-based concurrency model
- Asynchronous message passing
- Mailbox (message queue) per actor
- Stateful actors (behavior switching)
- Actor lifecycle support (start/stop events)
- TCP-based remote communication between actor systems

---

## Demo Application

The framework is demonstrated using a **distributed task processing system** consisting of:

- MasterActor – distributes tasks
- WorkerActor – executes tasks
- ResultCollectorActor – collects results

Example tasks include:
- arithmetic operations
- string processing
- simulated workload execution

---

## Architecture

Actors communicate exclusively through messages.  
Each actor processes messages from its own mailbox in an event loop.

The system supports running actors across multiple JVM instances using TCP communication.

---

## Technologies

- Java
- Maven
- TCP Sockets
- Java Concurrency (Threads, BlockingQueue)

---

## Running the Project

1. Start Worker node(s)
2. Start Master node
3. Submit tasks from Master

Each component can run in a separate process (or machine).

---
