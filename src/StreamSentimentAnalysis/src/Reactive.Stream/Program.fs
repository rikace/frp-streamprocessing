

open System
open System.Collections.Generic
open System.Configuration
open Akka
open Akka.Actor
open Akka.Streams
open Tweetinvi
open Tweetinvi.Models
open Akka.Streams.Dsl
open Shared.Reactive
open Shared.Reactive.Tweets
open System.Threading.Tasks
open TweeterStreaming
open Akka.FSharp.Actors
open Akka.FSharp

type RunnableGraphType =
    | TweetsToConsole
    | TweetsWithBroadcast
    | TweetsWithThrottle
    | TweetsWithWeather
    | TweetsWeatherWithThrottle
    | TweetsToEmotion
    
module Graph =
    let inline graph<'a>(tweetSource:Source<ITweet, 'a>) grapType =
        match grapType with
        // Simple implementation that read ~60Mb tweets from memory (load the tweets from local file-system) to generate high throughput (thus, it becomes easier to generate back-pressure)

        | RunnableGraphType.TweetsToConsole -> TweetsToConsole.create(tweetSource)
        // The Graph reads the source of events and generates 2 channels
        // 1 channel formats the output to render only the User of the tweet
        // 2 channel formats the output to render only the coordinates
        // Ultimately the channels are merged together and rendered at the same rate
        | RunnableGraphType.TweetsWithBroadcast -> TweetsWithBroadcast.create(tweetSource)
        // Similar to TweetsWithBroadcast but with Throttling
        | RunnableGraphType.TweetsWithThrottle -> TweetsWithThrottle.create(tweetSource)
        | RunnableGraphType.TweetsWithWeather -> TweetsWithWeather.create(tweetSource)
        | RunnableGraphType.TweetsWeatherWithThrottle -> TweetsWeatherWithThrottle.create(tweetSource)
        // Tweet Emotion in comparison to StockTicker example
        | _ -> failwithf "no implemented %O" grapType
       
let printer (inbox : Actor<_>) =
    let rec loop () = actor {
        let! msg = inbox.Receive()
        printfn "%s" msg
        return! loop ()
    }
    loop ()       

let runTweetStreaming useCachedTweets (graphType : RunnableGraphType) =
    use system = ActorSystem.Create("Reactive-System")


    Console.OutputEncoding <- System.Text.Encoding.UTF8
    Console.ForegroundColor <- ConsoleColor.Cyan

    Console.WriteLine("<< Press Enter to Start >>")
    Console.ReadLine() |> ignore

    let actorRef = spawn system "printer" printer
    
    let materialize = system.Materializer()

    if useCachedTweets then

        let tweetSource = Source.FromEnumerator(fun () -> (new TweetEnumerator(true)) :> IEnumerator<ITweet>)
        let graph = Graph.graph<NotUsed>(tweetSource) graphType actorRef.Tell
        graph.Run(materialize) |> ignore

    else
        Auth.SetCredentials(Credentials.twitterCredentials)

        // TODO OverflowStrategy.DropHead 
        let tweetSource = Source.ActorRef<ITweet>(100, OverflowStrategy.DropBuffer)
        let graph = Graph.graph<IActorRef>(tweetSource) graphType actorRef.Tell
        let actor = graph.Run(materialize)

        Utils.StartSampleTweetStream(actor)
    
    Console.WriteLine("<< Press Enter to Exit >>")
    Console.ReadLine() |> ignore
    
    

[<EntryPoint>]
let main argv =

    //Reactive.Tweets.Program.RunGraph()
    
    // WebCrawler.runWebCrawler()

    let runTweetStreamimg () =
       runTweetStreaming true RunnableGraphType.TweetsWeatherWithThrottle     
        //runTweetStreaming true RunnableGraphType.TweetsToConsole         
        //runTweetStreaming true RunnableGraphType.TweetsToEmotion         
        //runTweetStreaming true RunnableGraphType.TweetsWithThrottle       
        //runTweetStreaming true RunnableGraphType.TweetsWithBroadcast      
       
    runTweetStreamimg()
    
    Console.WriteLine("<< DONE!! >>")
    Console.ReadLine() |> ignore
    
    0
