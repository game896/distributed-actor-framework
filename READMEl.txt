### PING ###

1. Kreiraj example/taskpb/echo.proto
Napravi fajl sa:

syntax = "proto3";
package taskpb;
option go_package = "github.com/anomalyco/distributed-actor-framework/example/taskpb";

message EchoPayload {
  string text = 1;
}
2. Regeneriši proto
U C:\Agenti pokreni:

protoc --go_out=. --go_opt=module=github.com/anomalyco/distributed-actor-framework example/taskpb/echo.proto
3. Kreiraj example/actors/echo.go
package actors

import (
	"fmt"
	"github.com/anomalyco/distributed-actor-framework/actor"
	pb "github.com/anomalyco/distributed-actor-framework/example/taskpb"
)

type EchoActor struct{}

func NewEchoActor() *EchoActor {
	return &EchoActor{}
}

func (e *EchoActor) OnStart(ctx *actor.ActorContext) {
	fmt.Printf("[Echo] started at %s\n", ctx.Self())
	ctx.Become(func(ctx *actor.ActorContext, msg actor.Message) {
		if msg.Type == "echo" {
			payload := msg.Payload.(*pb.EchoPayload)
			fmt.Printf("[Echo] received: %s\n", payload.Text)
			ctx.Send(msg.Sender, actor.NewMessage(ctx.Self(), msg.Sender, "echo_reply", payload))
		}
	})
}

func (e *EchoActor) OnStop(ctx *actor.ActorContext) {}
4. Registruj tip u cmd/node/main.go
U postojeću init() funkciju dodaj:

internal.RegisterType("taskpb.EchoPayload", func() proto.Message {
	return &taskpb.EchoPayload{}
})
5. Dodaj novi mode u main() u cmd/node/main.go
Posle case "client": (oko linije 92) dodaj:

case "echo":
	runEcho(*addr)
6. Dodaj runEcho funkciju u isti fajl
func runEcho(addr string) {
	fmt.Printf("[Node] starting ECHO server on %s\n", addr)

	sys := actor.New("EchoSystem")
	mod, err := remote.StartModule(sys, addr)
	if err != nil {
		log.Fatalf("failed to start remote: %v", err)
	}
	defer mod.Stop()
	defer sys.Shutdown()

	sys.Spawn(actor.NewProps(actors.NewEchoActor(), nil), "echo")

	fmt.Printf("[Node] echo server ready on %s\n", mod.Address())
	select {}
}
7. Dodaj echo support u client
U runClient funkciju, posle slanja taskova, pošalji i echo poruku:

echoPID := actor.NewRemotePID("echo", "127.0.0.1:60060")
sys.Send(echoPID, actor.NewMessage(
	actor.PID{}, echoPID, "echo",
	&taskpb.EchoPayload{Text: "Zdravo svete!"},
))
8. Pokretanje
# Terminal 1 — echo server
go run ./cmd/node/ --mode=echo --addr=127.0.0.1:60060

# Terminal 2 — master + worker + client (ili samo client)
go run ./cmd/node/ --mode=client --master=127.0.0.1:50051 --tasks=5

###CALC###

1. Napravi example/taskpb/calc.proto:
syntax = "proto3";
package taskpb;
option go_package = "github.com/anomalyco/distributed-actor-framework/example/taskpb";

message CalcPayload {
  int32 value = 1;
}
2. Generiši Go kod:
protoc --go_out=. --go_opt=module=github.com/anomalyco/distributed-actor-framework example/taskpb/calc.proto
3. U cmd/node/main.go, u init() dodaj:
internal.RegisterType("taskpb.CalcPayload", func() proto.Message { return &taskpb.CalcPayload{} })
4. Napravi example/actors/calculator.go:
package actors

import (
	"fmt"

	"github.com/anomalyco/distributed-actor-framework/actor"
	"github.com/anomalyco/distributed-actor-framework/example/taskpb"
)

type CalculatorActor struct {
	value int
}

func NewCalculatorActor() *CalculatorActor {
	return &CalculatorActor{}
}

func (c *CalculatorActor) OnStart(ctx *actor.ActorContext) {
	fmt.Printf("[Calc] started, initial value = %d\n", c.value)
	ctx.Become(func(ctx *actor.ActorContext, msg actor.Message) {
		switch msg.Type {
		case "add":
			p := msg.Payload.(*taskpb.CalcPayload)
			c.value += int(p.Value)
			fmt.Printf("[Calc] +%d = %d\n", p.Value, c.value)
		case "sub":
			p := msg.Payload.(*taskpb.CalcPayload)
			c.value -= int(p.Value)
			fmt.Printf("[Calc] -%d = %d\n", p.Value, c.value)
		case "mul":
			p := msg.Payload.(*taskpb.CalcPayload)
			c.value *= int(p.Value)
			fmt.Printf("[Calc] *%d = %d\n", p.Value, c.value)
		case "get":
			ctx.Send(msg.Sender, actor.NewMessage(ctx.Self(), msg.Sender, "result", []byte(fmt.Sprintf("%d", c.value))))
		case "reset":
			c.value = 0
			fmt.Printf("[Calc] reset = 0\n")
		}
	})
}

func (c *CalculatorActor) OnStop(ctx *actor.ActorContext) {}
5. U cmd/node/main.go, zameni runCalc:
func runCalc(addr string) {
	fmt.Printf("[Node] starting CALC on %s\n", addr)

	sys := actor.New("CalcSystem")
	mod, err := remote.StartModule(sys, addr)
	if err != nil {
		log.Fatalf("failed to start remote: %v", err)
	}
	defer mod.Stop()
	defer sys.Shutdown()
	fmt.Printf("[Node] calc listening on %s\n", mod.Address())

	sys.Spawn(actor.NewProps(actors.NewCalculatorActor(), nil), "calc")

	calcPID2 := actor.NewRemotePID("calc", mod.Address())
	sys.Send(calcPID2, actor.NewMessage(actor.PID{}, calcPID2, "add", &taskpb.CalcPayload{Value: 10}))
	sys.Send(calcPID2, actor.NewMessage(actor.PID{}, calcPID2, "add", &taskpb.CalcPayload{Value: 5}))
	sys.Send(calcPID2, actor.NewMessage(actor.PID{}, calcPID2, "sub", &taskpb.CalcPayload{Value: 3}))
	sys.Send(calcPID2, actor.NewMessage(actor.PID{}, calcPID2, "mul", &taskpb.CalcPayload{Value: 2}))

	fmt.Println("[Node] calc demo done. Press Ctrl+C to stop.")
	select {}
}
6. Testiraj:
go run ./cmd/node/ --mode=calc --addr=:0
###LOGGER###

Korak 1: Napravi example/actors/logger.go
package actors

import (
	"fmt"
	"time"
	"github.com/anomalyco/distributed-actor-framework/actor"
)

type LoggerActor struct{}

func NewLoggerActor() *LoggerActor {
	return &LoggerActor{}
}

func (l *LoggerActor) OnStart(ctx *actor.ActorContext) {
	fmt.Printf("[Logger] started at %s\n", ctx.Self())
	ctx.Become(func(ctx *actor.ActorContext, msg actor.Message) {
		fmt.Printf("[Logger] %s | from=%s type=%s payload=%s\n",
			time.Now().Format("15:04:05"), msg.Sender, msg.Type, string(msg.Payload))
	})
}

func (l *LoggerActor) OnStop(ctx *actor.ActorContext) {}
Korak 2: U cmd/node/main.go — dodaj case
case "logger":
	runLogger(*addr)
Korak 3: Dodaj runLogger funkciju
func runLogger(addr string) {
	fmt.Printf("[Node] starting LOGGER on %s\n", addr)
	sys := actor.New("LoggerSystem")
	mod, err := remote.StartModule(sys, addr)
	if err != nil {
		log.Fatalf("failed to start remote: %v", err)
	}
	defer mod.Stop()
	defer sys.Shutdown()

	sys.Spawn(actor.NewProps(actors.NewLoggerActor(), nil), "logger")
	fmt.Printf("[Node] logger ready on %s\n", mod.Address())
	select {}
}
Korak 4: Slanje poruka
loggerPID := actor.NewRemotePID("logger", "127.0.0.1:60090")
sys.Send(loggerPID, actor.NewMessage(actor.PID{}, loggerPID, "INFO", []byte("Sistem pokrenut")))
sys.Send(loggerPID, actor.NewMessage(actor.PID{}, loggerPID, "WARN", []byte("Nema workera")))
Korak 5: Pokretanje
# Terminal 1
go run ./cmd/node/ --mode=logger --addr=127.0.0.1:60090
Razlika od calculator-a: Logger samo štampa poruke, ne menja stanje i ne odgovara.

### URL ###
package main

import (
	"fmt"
	"sync"
)

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

var (
	visited = make(map[string]bool)
	mu      sync.Mutex
	wg      sync.WaitGroup
)

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl(url string, depth int, fetcher Fetcher) {
	defer wg.Done()

	if depth <= 0 {
		return
	}

	mu.Lock()
	if visited[url] {
		mu.Unlock()
		return
	}
	visited[url] = true
	mu.Unlock()

	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("found: %s %q\n", url, body)

	for _, u := range urls {
		wg.Add(1)
		go Crawl(u, depth-1, fetcher)
	}
}

func main() {
	wg.Add(1)
	go Crawl("https://golang.org/", 4, fetcher)
	wg.Wait()
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}
### VEZBA 1 ###
package main

import (
	"fmt"
	"time"
)

func Double(in <-chan int, out chan<- int) {
	for data := range in {
		time.Sleep(100 * time.Millisecond) // imitiramo kompleksu računicu
		out <- data * 2
	}
}

func Increment(in <-chan int, out chan<- int) {
	for data := range in {
		time.Sleep(100 * time.Millisecond) // imitiramo kompleksu računicu
		out <- data + 1
	}
}

func main() {
	// Implementirati računanje pomoću obrazaca za kanale
	// Tako da se izvršava konkurentno
	chIn := make(chan int)
	chMid := make(chan int)
	chOut := make(chan int)

	go Double(chIn, chMid)
	go Increment(chMid, chOut)

	start := time.Now()

	go func() {
		for i := 1; i <= 5; i++ {
			chIn <- i
		}
		close(chIn)
	}()

	for i := 1; i <= 5; i++ {
		fmt.Println(<-chOut)
	}

	elapsed := time.Since(start)
	fmt.Println(elapsed)

}
### FIBONACCI ###
package main

import "fmt"

func fibonacci(ch, quit chan int) {
	x := 0
	y := 1
	for {
		select {
			case ch<-x:
				x, y = y, x+y
			case <-quit:
				return
		}
	}
}

func main() {
	ch := make(chan int)
	quit := make(chan int)
	
	go func() {for i:=0; i < 10; i++ {
			fmt.Println(<-ch)
		}
		quit <- 0
	}()
	fibonacci(ch, quit)
}
### TREE ###
package main

import (
	"fmt"
	"golang.org/x/tour/tree"
)

// Walk walks the tree t sending all values
// from the tree to the channel ch.
func Walk(t *tree.Tree, ch chan int) {
	if t == nil {
		return
	}

	Walk(t.Left, ch)
	ch <- t.Value
	Walk(t.Right, ch)
}

// Same determines whether the trees
// t1 and t2 contain the same values.
func Same(t1, t2 *tree.Tree) bool {
	ch1 := make(chan int)
	ch2 := make(chan int)

	go Walk(t1, ch1)
	go Walk(t2, ch2)

	for i := 0; i < 10; i++ {
		if <-ch1 != <-ch2 {
			return false
		}
	}

	return true
}

func main() {
	ch := make(chan int)

	go Walk(tree.New(1), ch)

	for i := 0; i < 10; i++ {
		fmt.Println(<-ch)
	}

	fmt.Println(Same(tree.New(1), tree.New(1)))
	fmt.Println(Same(tree.New(1), tree.New(2)))
}