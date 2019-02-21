module WebCrawler

open System
open System.Linq
open System.Collections.Immutable
open System.Text.RegularExpressions
open System.IO
open FSharp.Core
open System.Diagnostics
open System.Net
open System.Net.Http
open System.Threading
open System.Threading.Tasks
open Akka
open Akka.Actor
open Akka.FSharp
open Akka.Streams
open Akka.Streams.Dsl
open Akka.Streams.Implementation.Fusing
open Akka.Util
open Microsoft.FSharp.Core.Printf
open System.Threading.Tasks
open Microsoft.FSharp.Control
open Akkling
open Akkling.Streams
open CsQuery
open Akka.FSharp
open Akka.FSharp.Actors
open Akkling.Streams

let resolveLinks (uri : Uri, cq : CQ) = seq {
    for link in cq.["a[href]"] do
        let href = link.GetAttribute("href")
        let mutable uriResult = Unchecked.defaultof<Uri>
        if Uri.TryCreate(href, UriKind.Absolute, &uriResult) then
            yield uriResult
        elif Uri.TryCreate(uri, href, &uriResult) then
            yield uriResult
    }

let downloadPageAsync (uri : Uri) = async {
    printfn "downloading %O..." uri 
    let req = WebRequest.CreateHttp(uri)
    let! resp = req.GetResponseAsync() |> Async.AwaitTask
    use stream = resp.GetResponseStream()
    if isNull stream then return uri, CQ()
    else
        use reader = new StreamReader(stream)
        let! html = reader.ReadToEndAsync() |> Async.AwaitTask
        return uri, CQ.CreateDocument(html)
        }

    

let webCrawlerGraph () : IGraph<FlowShape<Uri, Uri>, NotUsed> =
    let index = ConcurrentSet<Uri>()
    let graph = Graph.create (fun b ->
    
        let merge = b.Add(new MergePreferred<Uri>(1))
        let bcast = b.Add(new Broadcast<Uri>(2))
        
        // async downlad page from provided uri resolve links from it        
        let flow =
            Flow.empty<Uri, _>
            |> Flow.filter(fun uri -> index.TryAdd(uri))
            |> Flow.asyncMapUnordered 4 downloadPageAsync
            |> Flow.async
            |> Flow.collect resolveLinks

        // feedback loop - take only those elements, which were successfully added to index (unique)
        let flowBack =
            Flow.empty<Uri, _>
            |> Flow.choose(fun uri -> if index.Contains uri then None else Some uri)
            |> Flow.conflateSeeded (fun (uris : ImmutableList<Uri>) uri -> uris.Add uri) (fun uri -> ImmutableList.Create(uri))
            |> Flow.collect id
        
        let pipe = b.From(merge).Via(flow).To(bcast)
        b.From(bcast).Via(flowBack).To(merge) |> ignore
        
        new FlowShape<Uri, Uri>(merge.In(0), bcast.Out(1)))    
    graph

let printer (inbox : Actor<_>) =
    let rec loop () = actor {
        let! msg = inbox.Receive()     
        printfn "%s" msg
        return! loop ()
    }
    loop ()
    
let run (urls : string seq) =
    let system = System.create("Reactive-WebCrawler") (Configuration.defaultConfig())
    let mat = system.Materializer()
  
    let actorRef = spawn system "printer" printer
    
    let graph : RunnableGraph<NotUsed> =
        printfn "Starting graph..."
        Graph.create (fun b ->
            let source = b.Add(Source.From( urls |> Seq.map Uri).Async())
            let sink = b.Add(Sink.forEach (fun s -> actorRef.Tell (sprintf "Uri processed : %O" s)))
            let crawlerFlow = b.Add(webCrawlerGraph())
            
            b.From(source).Via(crawlerFlow).To(sink) |> ignore
            
            ClosedShape.Instance)
        |> RunnableGraph.FromGraph
        
    graph |> Graph.run mat |> ignore
    Console.ReadLine() |> ignore
    


let runWebCrawler () =
    let sites = [
       "http://cnn.com/";          "http://bbc.com/"; 
       "http://www.live.com";      "http://www.fsharp.org";
       "http://news.live.com";     "http://www.digg.com";
       "http://www.yahoo.com";     "http://www.amazon.com"
       "http://news.yahoo.com";    "http://www.microsoft.com";
       "http://www.google.com";    "http://www.netflix.com";
       "http://news.google.com";   "http://www.maps.google.com";
       "http://www.bing.com";      "http://www.microsoft.com";
       "http://www.facebook.com";  "http://www.docs.google.com";
       "http://www.youtube.com";   "http://www.gmail.com";
       "http://www.reddit.com";    "http://www.twitter.com";   ]
    run sites
                    