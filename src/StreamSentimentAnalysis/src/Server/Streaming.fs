module ServerStreams

open Giraffe
open Saturn
open Shared
open Akka
open Akka.Actor
open Akka.FSharp
open Akka.Streams
open Akka.Streams.Dsl
open System
open System.Collections.Generic
open System.Threading.Tasks
open FSharp.Control.Tasks.V2
open Tweetinvi.Models
open System.Threading
open Shared.Reactive.Tweets
open System.Configuration
open System.IO
open Tweetinvi

type GraphType =
   | Sync
   | Async
   | Parallel
   
module Config =
    let consumerKey = ConfigurationManager.AppSettings.["ConsumerKey"]
    let consumerSecret = ConfigurationManager.AppSettings.["ConsumerSecret"]
    let accessToken = ConfigurationManager.AppSettings.["AccessToken"]
    let accessTokenSecret = ConfigurationManager.AppSettings.["AccessTokenSecret"]

type GrapSystem private () =
    static let instance = lazy (ActorSystem.Create("Reactive-System"))
    static let materialize =
        lazy(   let settings = ActorMaterializerSettings.Create(instance.Value).WithInputBuffer(4, 4)
                instance.Value.Materializer(settings))
    static member Instance = instance.Value 
    static member Materializer = materialize.Value 

module Temperature =
    open System.Linq
    open System.Xml.Linq
    
    let memoize f =
        let dict = System.Collections.Concurrent.ConcurrentDictionary()
        fun x -> dict.GetOrAdd(Some x, lazy (f x)).Force()
        
    let xn s = XName.Get s
    let getAsync (coordinates : ICoordinates) =
        async {
            use httpClient = new System.Net.WebClient()
            let requestUrl = sprintf "http://api.met.no/weatherapi/locationforecast/1.9/?lat=%f;lon=%f" coordinates.Latitude coordinates.Latitude
            printfn "%s" requestUrl

            let! result = httpClient.DownloadStringTaskAsync (Uri requestUrl) |> Async.AwaitTask
            let doc = XDocument.Parse(result)
            let temp = doc.Root.Descendants(xn "temperature").First().Attribute(xn "value").Value
            return Decimal.Parse(temp)
        }  
    
    let getFromFileAsync (coordinates : ICoordinates) = async {
        return! Shared.Reactive.Utils.GetWeatherAsync(coordinates) |> Async.AwaitTask
    }
        
    let getAsyncMemoized : ICoordinates -> Async<decimal> = getAsync |> memoize       
    let getFileAsyncMemoized : ICoordinates -> Async<decimal> = getFromFileAsync |> memoize       
        
module Graph =    
    open System
    open System
    open StreamingCombinators
    open Helpers
    open Akkling.Streams
    open Akkling.Streams
    open Analysis
    
    type MarkerMsg =
        | Marker of MarkerLocation
        | Markers of MarkerLocation array
    
    let printColor (marker : MarkerLocation) (msg : string) =
        let color = Console.ForegroundColor
        Console.ForegroundColor <-
            match marker.Emotion.toColor() with 
            | "red" -> ConsoleColor.Red
            | "green" -> ConsoleColor.Green
            | "blue" -> ConsoleColor.Cyan
            | _ -> ConsoleColor.Yellow
        printfn "%s" msg
        Console.ForegroundColor <- color
            
    let agentUpdate =
        MailboxProcessor.Start(fun inbox -> async {
                while true do
                    let! (msg : MarkerMsg) = inbox.Receive()
                    match msg with
                    | Marker m ->
                        let model = {Type = "pinsentiment"; Data = m}
                        let jsonData = Thoth.Json.Net.Encode.Auto.toString (2, model)
                        do! WebSockets.sendMessageToSockets jsonData |> Async.AwaitTask
                    | Markers ms ->
                        printfn "send data 2"
                        let model = {Type = "pinsentiments"; Data = ms}
                        let jsonData = Thoth.Json.Net.Encode.Auto.toString (2, model)
                        do! WebSockets.sendMessageToSockets jsonData |> Async.AwaitTask
                    })
  
    let create<'a> (tweetSource:Source<ITweet, 'a>) =        
        
        let runPrediction =
             let sentimentModel = ML.loadModel "../Data/model.zip"
             ML.runPrediction sentimentModel
             
        let scoreSentiment = runPrediction >> ML.scorePrediction >> normalize
        
        let graph = Graph.create(fun b ->
            
            let bcast = b.Add(new Broadcast<_>(2))
            
            let sentimentAnalysisFlow =
                Flow.Create<ITweet>()
                |> FlowEx.select (fun tweet ->
                     tweet, { zeroMarker with Emotion = { EmotionType.emotion = scoreSentiment tweet.Text  } })
                
            let flowCreateBy =
                Flow.Create<ITweet * MarkerLocation>()
                |> FlowEx.select (fun (tweet, marker) ->
                    tweet, { marker with Title = either tweet.CreatedBy.ScreenName tweet.CreatedBy.Name })
                
            let coordinateFlow =
                Flow.Create<ITweet * MarkerLocation>()
                |> FlowEx.select (fun (tweet, marker) ->
                    tweet, 
                    { marker with
                        Lat = tweet.Coordinates.Latitude
                        Lng = tweet.Coordinates.Longitude
                        Color = marker.Emotion.toColor()
                    })
                
            let temperatureFlow =
                Flow.Create<ITweet * MarkerLocation>()
                |> FlowEx.selectAsync 1 (fun (tweet, marker) -> task {
                    let! temperature = Temperature.getFileAsyncMemoized tweet.Coordinates
                    return tweet, 
                    { marker with
                        Temperature = temperature
                    }
                 })

            let writeSink =
                 Sink.ForEach<ITweet * MarkerLocation>(fun (tweet, marker) -> 
                    printColor marker (sprintf "[ %s ] - Tweet [ %s ]" (string marker.Emotion) tweet.Text)
                 )
                 
            let updateSink =
                 Sink.ForEach<ITweet * MarkerLocation>(fun (tweet, emotion) ->  
                 agentUpdate.Post (Marker emotion)// update UI
                 )

            b.From(tweetSource).Via(sentimentAnalysisFlow).Via(flowCreateBy).Via(coordinateFlow).To(bcast) |> ignore
            b.From(bcast.Out(0)).To(writeSink) |> ignore
            b.From(bcast.Out(1)).To(updateSink) |> ignore
          
            ClosedShape.Instance)              
        
        graph |> RunnableGraph.FromGraph

    
    let createAsync<'a> (tweetSource:Source<ITweet, 'a>) =        
       
        let runPrediction =
             let sentimentModel = ML.loadModel "../Data/model.zip"
             ML.runPrediction sentimentModel
             
        let scoreSentiment = runPrediction >> ML.scorePrediction >> normalize
        
        let graph = Graph.create(fun b ->
            
            let bcast = b.Add(new Broadcast<_>(2))
            
            let formatFlow =
                Flow.Create<ITweet>()
                |> FlowEx.select (fun tweet ->
                    tweet, { zeroMarker with Emotion = { EmotionType.emotion = scoreSentiment tweet.Text  } })
                |> FlowEx.async
                
            let flowCreateBy =
                Flow.Create<ITweet * MarkerLocation>()
                |> FlowEx.select (fun (tweet, marker) ->
                    tweet, { marker with Title = either tweet.CreatedBy.ScreenName tweet.CreatedBy.Name })
                |> FlowEx.async
                
            let coordinateFlow =
                Flow.Create<ITweet * MarkerLocation>()
                |> FlowEx.select (fun (tweet, marker) ->
                    tweet, 
                    { marker with
                        Lat = tweet.Coordinates.Latitude
                        Lng = tweet.Coordinates.Longitude
                        Color = marker.Emotion.toColor()
                    })
                |> FlowEx.async
                
            let temperatureFlow =
                Flow.Create<ITweet * MarkerLocation>()
                |> FlowEx.selectAsync 1 (fun (tweet, marker) -> task {
                    // let! temperature = Temperature.getAsyncMemoized tweet.Coordinates
                    let! temperature = Temperature.getFileAsyncMemoized tweet.Coordinates
                    return tweet, 
                    { marker with Temperature = temperature }
                 })
                 |> FlowEx.async
                 
            let writeSink =
                 Sink.ForEach<ITweet * MarkerLocation>(fun (tweet, marker) -> 
                    printColor marker (sprintf "[ %s ] - Tweet [ %s ]" (string marker.Emotion) tweet.Text)
                 )
                 
            let updateSink =
                 Sink.ForEach<ITweet * MarkerLocation>(fun (tweet, emotion) ->  
                 agentUpdate.Post (Marker emotion)
                 )
                        
            //b.From(tweetSource).Via(formatFlow).Via(flowCreateBy).Via(coordinateFlow).To(bcast) |> ignore
            b.From(tweetSource).Via(formatFlow).Via(flowCreateBy).Via(temperatureFlow).Via(coordinateFlow).To(bcast) |> ignore
            b.From(bcast.Out(0)).To(writeSink) |> ignore
            b.From(bcast.Out(1)).To(updateSink) |> ignore
            
            ClosedShape.Instance)
              
        graph |> RunnableGraph.FromGraph              

    let createParallelAsync<'a> parallelism (tweetSource:Source<ITweet, 'a>) =        
        
        let runPrediction =
             let sentimentModel = ML.loadModel "../Data/model.zip"
             ML.runPrediction sentimentModel
             
        let scoreSentiment = runPrediction >> ML.scorePrediction >> normalize
             
        let graph = Graph.create(fun b ->
            
            let bcast = b.Add(new Broadcast<_>(2))
            
            let formatFlow =
                Flow.Create<ITweet>()
                |> FlowEx.select (fun tweet ->
                    tweet, { zeroMarker with Emotion = { EmotionType.emotion = scoreSentiment tweet.Text  } })
                |> FlowEx.async
                
            let flowCreateBy =
                Flow.Create<ITweet * MarkerLocation>()
                |> FlowEx.select (fun (tweet, marker) ->
                    tweet, { marker with Title = either tweet.CreatedBy.ScreenName tweet.CreatedBy.Name })
                |> FlowEx.async
                
            let coordinateFlow =
                Flow.Create<ITweet * MarkerLocation>()
                |> FlowEx.asyncMapUnordered parallelism (fun (tweet, marker) -> async {
                    return tweet,
                     { marker with
                        Lat = tweet.Coordinates.Latitude
                        Lng = tweet.Coordinates.Longitude
                        Color = marker.Emotion.toColor()
                    }})
                |> FlowEx.async
                
            let temperatureFlow =
                Flow.Create<ITweet * MarkerLocation>()
                |> FlowEx.asyncMapUnordered parallelism (fun (tweet, marker) -> async {
                    // let! temperature = Temperature.getAsyncMemoized tweet.Coordinates
                    let! temperature = Temperature.getFileAsyncMemoized tweet.Coordinates
                    return tweet, 
                    { marker with Temperature = temperature }
                 })
                 |> FlowEx.async
                 
            let writeSink =
                 Sink.ForEach<ITweet * MarkerLocation>(fun (tweet, marker) -> 
                    printColor marker (sprintf "[ %s ] - Tweet [ %s ]" (string marker.Emotion) tweet.Text)
                 )
                 
            let updateSink =
                 Sink.ForEach<ITweet * MarkerLocation>(fun (tweet, emotion) ->  
                 agentUpdate.Post (Marker emotion)
                 )
                        
            //b.From(tweetSource).Via(formatFlow).Via(flowCreateBy).Via(coordinateFlow).To(bcast) |> ignore
            b.From(tweetSource).Via(formatFlow).Via(flowCreateBy).Via(temperatureFlow).Via(coordinateFlow).To(bcast) |> ignore
            b.From(bcast.Out(0)).To(writeSink) |> ignore
            b.From(bcast.Out(1)).To(updateSink) |> ignore
            
            ClosedShape.Instance)              
        
        graph |> RunnableGraph.FromGraph
        
    let createParallelAsyncGrouped<'a> parallelism (tweetSource:Source<ITweet, 'a>) =        
        
        let runPrediction =
             let sentimentModel = ML.loadModel "../Data/model.zip"
             ML.runPrediction sentimentModel
             
        let scoreSentiment = runPrediction >> ML.scorePrediction >> normalize
        
        let graph = Graph.create(fun b ->
            
            let bcast = b.Add(new Broadcast<_>(2))
            
            let formatFlow : Flow<ITweet, (ITweet * MarkerLocation), NotUsed>=
                Flow.Create<ITweet>()
                |> FlowEx.asyncMapUnordered parallelism (fun tweet -> async {
                    return tweet, { zeroMarker with Emotion = { EmotionType.emotion = scoreSentiment tweet.Text  } }})
                |> FlowEx.async
                
            let flowCreateBy =
                Flow.Create<ITweet * MarkerLocation>()
                |> FlowEx.asyncMapUnordered parallelism (fun (tweet, marker) -> async {
                    return tweet, { marker with Title = either tweet.CreatedBy.ScreenName tweet.CreatedBy.Name }})
                |> FlowEx.async
                
            let coordinateFlow =
                Flow.Create<ITweet * MarkerLocation>()
                |> FlowEx.asyncMapUnordered parallelism (fun (tweet, marker) -> async {
                    return tweet, 
                        { marker with
                            Lat = tweet.Coordinates.Latitude
                            Lng = tweet.Coordinates.Longitude
                            Color = marker.Emotion.toColor()
                        }})
                |> Flow.groupedWithin 10 (TimeSpan.FromSeconds(2.))
    
            let temperatureFlow =
                Flow.Create<ITweet * MarkerLocation>()
                |> FlowEx.asyncMapUnordered parallelism (fun (tweet, marker) -> async {
                    let! temperature = Temperature.getAsyncMemoized tweet.Coordinates
                    return tweet, { marker with Temperature = temperature }
                 })
                |> FlowEx.async

            let writeSink =
                 Sink.ForEach<(ITweet * MarkerLocation) seq>(fun tweets ->
                    for (tweet, marker) in tweets do
                        printColor marker (sprintf "[ %s ] - Tweet [ %s ]" (string marker.Emotion) tweet.Text)
                 )
                 
            let updateSink =
                 Sink.ForEach<(ITweet * MarkerLocation) seq>(fun tweets ->
                    printfn "send data 1"
                    tweets |> Seq.map snd |> Seq.toArray |> Markers |> agentUpdate.Post )
           
            b.From(tweetSource).Via(formatFlow).Via(flowCreateBy).Via(coordinateFlow).To(bcast) |> ignore
            b.From(bcast.Out(0)).To(writeSink) |> ignore
            b.From(bcast.Out(1)).To(updateSink) |> ignore
            
            ClosedShape.Instance)              
        
        graph |> RunnableGraph.FromGraph


let startStreamingCache (graphType : GraphType) =
   
    let materialize = GrapSystem.Materializer
    let source = new TweetEnumerator(true)    
    let tweetSource = Source.FromEnumerator(fun () -> source :> IEnumerator<ITweet>)
    
    let graph =
        match graphType with  
        | Sync -> Graph.create<NotUsed> tweetSource
        | Async -> Graph.createAsync<NotUsed> tweetSource
        | Parallel -> Graph.createParallelAsync<NotUsed> 8 tweetSource
        
    graph.Run(materialize) |> ignore
    
    { new IDisposable with
          member __.Dispose() =
              source.Dispose()
              materialize.Dispose() }
    
let startStreamingLive update =
    let materialize = GrapSystem.Materializer
    
    Auth.SetCredentials(new TwitterCredentials(Config.consumerKey, Config.consumerSecret, Config.accessToken, Config.accessTokenSecret))

    let tweetSource = Source.ActorRef<ITweet>(100, OverflowStrategy.DropBuffer)
    let graph = Graph.create<IActorRef> tweetSource
    
    // let graph = Graph.createAsync<IActorRef> update tweetSource
    // let graph = Graph.createParallelAsync<IActorRef> update 4 tweetSource
    
    let actor = graph.Run(materialize)

    Shared.Reactive.Utils.StartSampleTweetStream(actor)
    
    actor
    