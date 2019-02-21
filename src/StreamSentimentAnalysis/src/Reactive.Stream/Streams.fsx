#r "../../packages/server/Akka/lib/netstandard1.6/Akka.dll"
#r "../../packages/server/Akka.Streams/lib/netstandard1.6/Akka.Streams.dll"
#r "../../packages/server/Akka.FSharp/lib/netstandard2.0/Akka.FSharp.dll"
#r "../../packages/server/Akkling/lib/netstandard1.6/Akkling.dll"
#r "../../packages/server/Akkling.Streams/lib/netstandard1.6/Akkling.Streams.dll"

open System
open Akka.Streams
open Akka.Streams.Dsl
open Akkling
open Akkling.Streams
open Akkling.Behaviors
open System.Linq
open Akka
open System.Numerics
open System.Text.RegularExpressions
open System.IO
open System.Threading.Tasks

System.Environment.CurrentDirectory <- __SOURCE_DIRECTORY__

let text = """
       Lorem Ipsum is simply dummy text of the printing and typesetting industry.
       Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,
       when an unknown printer took a galley of type and scrambled it to make a type
       specimen book."""

let system = System.create "streams-sys" <| Configuration.defaultConfig()
let mat = system.Materializer()


// 1
let source = Source.From(Enumerable.Range(1,100))
source.RunForeach(Action<_>(fun i -> Console.WriteLine(i.ToString())), mat)


// 2
let processText source =
    source
    |> Source.map (fun (x:string) -> x.ToUpper())
    |> Source.filter (String.IsNullOrWhiteSpace >> not)
    |> Source.runForEach mat (printfn "%s")

Source.ofArray (text.Split()) |> processText |> Async.Start


// 3
// val behavior : targetRef:ICanTell<'a> -> m:Actor<'a> -> Effect<'a>
let behavior (targetRef:ICanTell<_>) (m:Actor<_>) =
    let rec loop () = actor {
        let! msg = m.Receive ()
        targetRef <! msg
        return! loop ()
    }
    loop ()

// val spawnActor : targetRef:ICanTell<'a> -> IActorRef<'a>
let spawnActor targetRef =
    spawnAnonymous system <| props (behavior targetRef)

let processTextCombined : IActorRef<string> =
        Source.actorRef OverflowStrategy.DropNew 1000
        |> Source.mapMaterializedValue(spawnActor)
        |> Source.collect(fun (text:string) -> text.Split())
        |> Source.filter (String.IsNullOrWhiteSpace >> not)
        |> Source.map (fun x -> x.ToUpper())
        |> Source.toMat(Sink.forEach(fun s -> printfn "Received: %s" s)) Keep.left
        |> Graph.run mat

processTextCombined <! text


let largeSource = Source.From(Enumerable.Range(1,1000000000))

let factorials = largeSource.Scan(BigInteger(1), Func<_,_,_>(fun acc next -> acc * (BigInteger next)))

factorials
     .ZipWith(Source.From(Enumerable.Range(0, 1000000000)), Func<_,_,_>(fun num idx -> sprintf "%d ! = %O" idx num))
     .Throttle(1, TimeSpan.FromMilliseconds(50.), 1, ThrottleMode.Shaping)
     .RunForeach(Action<_>(fun s -> Console.WriteLine(s)), mat)





let regex = Regex(@"\{.*(?<name>Microsoft.*)\|\]", RegexOptions.ExplicitCapture ||| RegexOptions.Compiled)
let normalizeName (name: string) = String.Join(".", name.Split([|';'; '"'; ' '|], StringSplitOptions.RemoveEmptyEntries))

let materializer = Configuration.defaultConfig() |> System.create "test" |> ActorMaterializer.Create

let searchWithAkka (file: string) =
    use reader = new StreamReader(file)
    Source
        .UnfoldAsync(0, fun _ -> reader.ReadLineAsync().ContinueWith(fun (x: Task<string>) -> 0, x.Result))
        .Take(1000000L)
        .SelectAsyncUnordered(4, fun line ->
            Task.Factory.StartNew(fun _ ->
                match regex.Match line with
                | m when m.Success -> m.Groups.["name"].Value
                | _ -> ""))
        .Where(fun x -> x <> "")
        .SelectAsyncUnordered(4, fun (name: string) -> Task.Factory.StartNew(fun _ -> normalizeName name))
        .RunAggregate(ResizeArray(), (fun acc x -> acc.Add x; acc), materializer)
        .Result
        .Count

searchWithAkka @"../Data/big.txt"

