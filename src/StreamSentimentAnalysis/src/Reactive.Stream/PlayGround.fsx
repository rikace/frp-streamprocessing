#r "../../packages/server/Akka/lib/netstandard1.6/Akka.dll"
#r "../../packages/server/Akka.Streams/lib/netstandard1.6/Akka.Streams.dll"
#r "../../packages/server/Akka.FSharp/lib/netstandard2.0/Akka.FSharp.dll"
#r "../../packages/server/Akkling/lib/netstandard1.6/Akkling.dll"
#r "../../packages/server/Akkling.Streams/lib/netstandard1.6/Akkling.Streams.dll"

open System
open System.Text.RegularExpressions
open System.IO
open FSharp.Core
open System.Diagnostics
open System.Net
open System.Threading
open System.Threading.Tasks
open Akka
open Akka.Actor
open Akka.FSharp
open Akka.Streams
open Akka.Streams.Dsl
open Akka.Streams.Implementation.Fusing
open Microsoft.FSharp.Core.Printf
open System.Threading.Tasks
open Microsoft.FSharp.Control
open Akkling
open Akkling.Streams


[<AutoOpen>]
module Utils =
    let (==>) (src: Source<_,_>) flow = src.Via(flow)
    let (=>|) (src: Source<_,_>) sink = src.To(sink)
    let print fmt = kprintf (printfn "%O %s" DateTime.Now) fmt

let rec fib n = if n < 2 then n else fib (n - 1) + fib (n - 2)

let tc f =
    let sw = Stopwatch.StartNew()
    f() |> ignore
    sw.Stop()
    printfn "Elapsed %O" sw.Elapsed

module Source =
    let inline ofSeq x = Source.From x
    let inline buffer size strategy (src: Source<_,_>) = src.Buffer(size, strategy)
    let inline map (mapper: 'a -> 'b) (src: Source<_,_>) = src.Select(fun x -> mapper x)

    let inline mapAsync parallelism (mapper: 'a -> Async<'b>) (src: Source<_,_>) =
        src.SelectAsync(parallelism, fun x -> Async.StartAsTask (mapper x))


let text = """
       Lorem Ipsum is simply dummy text of the printing and typesetting industry.
       Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,
       when an unknown printer took a galley of type and scrambled it to make a type
       specimen book."""

let system = ActorSystem.Create("Reactive-System")
let materializer = system.Materializer()
// 1. Basic stream transformation
Source.ofArray (text.Split())
|> Source.map (fun x -> x.ToUpper())
|> Source.filter (String.IsNullOrWhiteSpace >> not)
|> Source.runForEach materializer (printfn "%s")
|> Async.RunSynchronously


//let helloWorld : RunnableGraph<_> =    
//  Source.Single("Hello world")
//      .Via(Flow.Create<string>().Select(fun str -> str.ToUpper()))
//      .To(Sink.forEach(fun s -> printfn "Received: %s" s))
//
//let helloWorld : RunnableGraph<_> =    
//  Source.Single("Hello world")
//      .map(fun str -> str.ToUpper())
//      .to(Sink.forEach(printfn "Received: %s"))

// Actor Interop
let behavior (targetRef:ICanTell<'b>) (m:Actor<'a>) =
    let rec loop () = actor {
        let! msg = m.Receive ()
        printfn "Actor received : %O" msg
        targetRef <! msg
        return! loop ()
    }
    loop ()

let spawnActor targetRef =
    spawnAnonymous system <| props (behavior targetRef)

let s = Source.actorRef OverflowStrategy.DropNew 1000
        |> Source.mapMaterializedValue(spawnActor)
        |> Source.toMat(Sink.forEach(fun s -> printfn "Received: %s" s)) Keep.left
        |> Graph.run materializer

s <! "My Message"







// Dynamic streams
let sink = Sink.forEach (printfn "%s")
let consumer =
    Source.mergeHub 10
    |> Source.toMat sink Keep.left
    |> Graph.run materializer

Source.singleton "hello" |> Source.runWith materializer consumer
Source.singleton "world" |> Source.runWith materializer consumer


["google.com"; "ya.ru"; "microsoft.com"; "jet.com"; "ycombinator.com"]
|> Source.ofSeq
|> Source.map (fun x -> Uri ("http://" + x))
|> Source.asyncMapUnordered 10 (fun uri ->
    async {
        let client = new WebClient()
        let! content = client.DownloadStringTaskAsync uri |> Async.AwaitTask
        return uri, content
    })
|> Source.map (fun x -> printfn "%O %O downloaded" DateTime.Now (fst x); x)
|> Source.buffer 3 OverflowStrategy.Backpressure
|> Source.runForEach materializer (fun (url, content) ->
    printfn "%O %O => %d bytes" DateTime.Now url content.Length
    Thread.Sleep 1000)
|> Async.RunSynchronously




let praseFile filePath =
    let regex = Regex(@"\{.*(?<name>Microsoft.*)\|\]", RegexOptions.ExplicitCapture ||| RegexOptions.Compiled)
    let normalizeName (name: string) = String.Join(".", name.Split([|';'; '"'; ' '|], StringSplitOptions.RemoveEmptyEntries))

    let materializer = Configuration.defaultConfig() |> System.create "test" |> ActorMaterializer.Create

    let searchWithAkka (file: string) =
        use reader = new StreamReader(file)
        Source
            .UnfoldAsync(0, fun _ -> reader.ReadLineAsync().ContinueWith(fun (x: Task<string>) -> 0, x.Result))
            .Take(1000000L)
            .SelectAsync(4, fun line ->
                Task.Factory.StartNew(fun _ ->
                    match regex.Match line with
                    | m when m.Success -> m.Groups.["name"].Value
                    | _ -> ""))
            .Where(fun x -> x <> "")
            .SelectAsync(4, fun (name: string) -> Task.Factory.StartNew(fun _ -> normalizeName name))
            .RunAggregate(ResizeArray(), (fun acc x -> acc.Add x; acc), materializer)
            .Result
            .Count

    searchWithAkka filePath //@"d:\downloads\big.txt"


let streamEx () =
    let system = System.create "test" (Configuration.defaultConfig())
    let mat = system.Materializer()

    let balancer (worker : Flow<_,_,_>) count =
        Graph.create (fun b ->
            let balancer = b.Add(Balance(count, waitForAllDownstreams = true))
            let merge = b.Add(Merge<_> count)
            for _ in 1..count do
                b.From(balancer).Via(worker).To(merge) |> ignore
            FlowShape(balancer.In, merge.Out)
        ) |> Flow.FromGraph

    let worker = Flow.id |> Flow.map (fun x -> x + 100)


    [1..5]
    |> Source.ofList
    |> Source.via (balancer worker 3)
    |> Source.runForEach mat (printfn "%d")
    |> Async.RunSynchronously


// CPU consumption test

// Simulate a CPU-intensive workload that takes ~10 milliseconds
let spin (value: int) =
    let start = DateTime.Now.Ticks
    while DateTime.Now.Ticks - start < int64 value do
        Thread.Yield() |> ignore
    value

let spawn f x = async { return f x }


// This implementation will execute synchronously, on a single thread, and take approximately twice as long as the following implementation,
// which will consume two threads, performing the CPU-intensive operations concurrently.
let count = 3000
#time
let runSync() =
    Source.ofList [1..count]
    |> Source.map(spin)
    |> Source.map(spin)
    |> Source.runWith materializer Sink.ignore

runSync() |> Async.RunSynchronously

// the following stream captures the simulated, CPU-bound workload in a Future and will execute just as fast as using async between the two map stages.
let runAsyncBoundries() =
    Source.ofList [1..count]
    |> Source.map(spin)
    |> Source.async
    |> Source.map(spin)
    |> Source.async
    |> Source.runWith materializer Sink.ignore

runAsyncBoundries() |> Async.RunSynchronously

// With this approach, however, the mapAsync parallelism can be increased further, to increase the throughput even more. On an eight-core machine, the following example will run four-times faster still.
let runAsyncFutures() =
    Source.ofList [1..count]
    |> Source.asyncMap 1 (fun i -> spawn spin i)
    |> Source.asyncMap 1 (fun i -> spawn spin i)
    |> Source.runWith materializer Sink.ignore

runAsyncFutures() |> Async.RunSynchronously

let runAsyncParallelFutures () =
    let n = 4
    Source.ofList [1..count]
    |> Source.asyncMap n (fun i -> spawn spin i)
    |> Source.asyncMap n (fun i -> spawn spin i)
    |> Source.runWith materializer Sink.ignore

runAsyncParallelFutures() |> Async.RunSynchronously

let runAsyncParallelBuffer () =
    let n = 4
    Source.ofList [1..count]
    |> Source.asyncMap n (fun i -> spawn spin i)
    |> Source.buffer 16 OverflowStrategy.Backpressure
    |> Source.asyncMap n (fun i -> spawn spin i)
    |> Source.buffer 16 OverflowStrategy.Backpressure
    |> Source.runWith materializer Sink.ignore  
  

module AgentEx =
    
    [<Interface>]
    type IAgent =
        abstract Tell : 'a -> unit
        abstract Ask : 'a -> Async<'b>
    
    let parellelWorker n f =
        let agents = Array.init n (fun _ -> MailboxProcessor.Start(f))
        MailboxProcessor.Start(fun inbox ->
            let rec loop index = async {
                let! msg = inbox.Receive()
                agents.[index].Post msg
                return! loop ((index + 1) % n)
            }
            loop 0)
           
     
    let pipe (a : MailboxProcessor<_>) (b : MailboxProcessor<_>) =
        
        ()
   
    let broadcast (source : IAgent) f (destinations : IAgent []) =
        MailboxProcessor.Start(fun inbox -> 
            let rec loop () = async {
                let! msg = inbox.Receive()
                let! res = f msg
                for agent in destinations do
                    agent.Tell res
                return! loop ()
            }
            loop ())
        
