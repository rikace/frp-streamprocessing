module TweeterStreaming

open System
open System.Collections.Generic
open FSharp.Core
open System.Threading.Tasks
open Akka
open Akka.Actor
open Akka.FSharp
open Akka.Streams
open Akka.Streams.Dsl
open Microsoft.FSharp.Core.Printf
open Microsoft.FSharp.Control
open Akkling
open Akkling.Streams
open System.Configuration
open System.Text
open Tweetinvi.Models
open Tweetinvi
open Shared.Reactive
open System.Linq
open System.Xml.Linq
open StreamingCombinators

module Credentials =
    let consumerKey = ConfigurationManager.AppSettings.["ConsumerKey"]
    let consumerSecret = ConfigurationManager.AppSettings.["ConsumerSecret"]
    let accessToken = ConfigurationManager.AppSettings.["AccessToken"]
    let accessTokenSecret = ConfigurationManager.AppSettings.["AccessTokenSecret"]
    
    let twitterCredentials = new TwitterCredentials(consumerKey, consumerSecret, accessToken, accessTokenSecret)
    

// Function overloading using infix operator
// Function overloading is  a syntactic construct where we define two or more different functions with the same name
type SinkHelper = SinkHelper with
    static member (==>) (_:SinkHelper, a:GraphDsl.ForwardOps<_, NotUsed>) = fun(b:Inlet<string>) -> a.To(b)
    static member (==>) (_:SinkHelper, a:Sink<_,Task>) = fun(b:Source<string, _>) -> b.To(a)
    static member (==>) (_:SinkHelper, a:Source<_,_>) = fun(b:Sink<_,Task>) ->a.To(b)
    static member (==>) (_:SinkHelper, b:Inlet<string>) = fun(a:GraphDsl.ForwardOps<_, NotUsed>) -> a.To(b)
let inline sink x = SinkHelper ==> x


type MatHelpler = MatHelpler with
    static member (==>) (_:MatHelpler, a:IGraph<FlowShape<_, _>, NotUsed>) = fun (b:Source<ITweet, _>) -> b.Via(a)
let inline mat x = MatHelpler ==> x

type ViaHelper = ViaHelper with
    static member (==>) (_:ViaHelper, a:IGraph<FlowShape<_, _>, NotUsed>) = fun (b:GraphDsl.ForwardOps<_, NotUsed>) -> b.Via(a)
    static member (==>) (b:ViaHelper, a:GraphDsl.ForwardOps<_, NotUsed>) = fun (x:IGraph<FlowShape<_, _>, NotUsed>) -> a.Via(x)
    static member (==>) (b:ViaHelper, a:Source<ITweet, _>) = fun(x:IGraph<FlowShape<_, _>,_>) -> a.Via(x)
let inline via x = ViaHelper ==> x

[<RequireQualifiedAccess>]
module Forecast =
    let xn s = XName.Get s
    let getWeatherAsync(coordinates:ICoordinates) =
        async {
            use httpClient = new System.Net.WebClient()
            let requestUrl = sprintf "http://api.met.no/weatherapi/locationforecast/1.9/?lat=%f;lon=%f" coordinates.Latitude coordinates.Latitude
            printfn "%s" requestUrl

            let! result = httpClient.DownloadStringTaskAsync (Uri requestUrl) |> Async.AwaitTask
            let doc = XDocument.Parse(result)
            let temp = doc.Root.Descendants(xn "temperature").First().Attribute(xn "value").Value
            return Decimal.Parse(temp)
        } |> Async.StartAsTask
        
    let getWeatherFromFileAsync(coordinates : ICoordinates) =
        async {
            return!  Utils.GetWeatherAsync(coordinates) |> Async.AwaitTask
        } |> Async.StartAsTask
    
        
module TweetsWeatherWithThrottle =

    let create<'a>(tweetSource:Source<ITweet, 'a>) effect : IRunnableGraph<'a> =
        
        let formatUser =
            Flow.Create<IUser>()
            |> FlowEx.select (Utils.FormatUser)

        let formatTemperature =
            Flow.Create<decimal>()
            |> FlowEx.select (Utils.FormatTemperature)

        let writeSink =
            Sink.ForEach<string>(fun msg -> effect msg)

      
        let graph = GraphDsl.Create(fun buildBlock ->
            let broadcast = buildBlock.Add(Broadcast<ITweet>(2))
            let merge = buildBlock.Add(Merge<string>(2))

            buildBlock.From(broadcast.Out(0))
            |> via (Flow.Create<ITweet>()
                    |> FlowEx.select (fun tweet -> tweet.CreatedBy)
                    |> FlowEx.throttle 10 (TimeSpan.FromSeconds(1.)) 1 ThrottleMode.Shaping)
            |> via (formatUser)
            |> sink (merge.In(0))
            |> ignore

            buildBlock.From(broadcast.Out(1))
            |> via (Flow.Create<ITweet>()
                    |> FlowEx.select (fun tweet -> tweet.Coordinates)
                    
                // 1- Throttle (Throttle(10) >> same rate
                // 2- Throttle (Throttle(1))
                //         2 channels with a difference throttle values
                //         1 request with 10 msg per second and 1 request with 1 msg per second
                //         because there is only 1 stream source, it cannot send messages to a
                //         different rate, thus, it satisfies the lowest requirement.
                    
                    |> FlowEx.throttle 10 (TimeSpan.FromSeconds(1.)) 1 ThrottleMode.Shaping)
                    |> via (Flow.Create<ICoordinates>()
                             // selectAsync - Runs in parallel up to 4
                             |> FlowEx.selectAsync 1 (fun c -> Forecast.getWeatherFromFileAsync c))
            |> via (formatTemperature)
            |> sink (merge.In(1))
            |> ignore

            FlowShape<ITweet, string>(broadcast.In, merge.Out))

        tweetSource
        |> FlowEx.where (Predicate(fun (tweet:ITweet) -> not(isNull tweet.Coordinates)))
        |> mat graph
        |> sink writeSink


    
module TweetsToConsole =
    open FlowEx
    // Simple implementation that read ~70Mb tweets from memory (load the tweets from local file-system) to generate high throughput
    
    let inline create<'a>(tweetSource : Source<ITweet, 'a>) effect : IRunnableGraph<'a> =

        let formatFlow =
            Flow.Create<ITweet>()
            |> select (Utils.FormatTweet)

        let writeSink = Sink.ForEach<string>(fun text -> effect text)

        tweetSource.Via(formatFlow).To(writeSink)

module TweetsWithBroadcast =
   open FlowEx
    
   let inline create(tweetSource:Source<ITweet, 'a>) effect =
        let formatUser =
            Flow.Create<IUser>()
            |> select (Utils.FormatUser)

        let formatCoordinates =
            Flow.Create<ICoordinates>()
            |> select (Utils.FormatCoordinates)

        let flowCreateBy =
            Flow.Create<ITweet>()
            |> select (fun tweet -> tweet.CreatedBy)

        let flowCoordinates =
            Flow.Create<ITweet>()
            |> select (fun tweet -> sprintf "Lat %f - Lng %f" tweet.Coordinates.Latitude tweet.Coordinates.Longitude)

        let writeSink = Sink.ForEach<string>(fun text -> effect text)

        let graph = GraphDsl.Create(fun buildBlock ->
            let broadcast = buildBlock.Add(Broadcast<ITweet>(2))
            let merge = buildBlock.Add(Merge<string>(2))

            buildBlock.From(broadcast.Out(0)).Via(flowCreateBy).Via(formatUser).To(merge.In(0)) |> ignore
            buildBlock.From(broadcast.Out(1)).Via(flowCoordinates).To(merge.In(1)) |> ignore
            FlowShape<ITweet, string>(broadcast.In, merge.Out))
        
         
        tweetSource
        |> where (Predicate(fun (tweet:ITweet) -> not(isNull tweet.Coordinates)))
        |> viaGraph graph
        |> sinkGraph writeSink

module TweetsWithThrottle =

    let create<'a>(tweetSource:Source<ITweet, 'a>) effect =
        let formatUser =
            Flow.Create<IUser>()
            |> FlowEx.select (Utils.FormatUser)

        let formatCoordinates =
            Flow.Create<ICoordinates>()
            |> FlowEx.select (Utils.FormatCoordinates)

        let flowCreateBy =
            Flow.Create<ITweet>()
            |> FlowEx.select (fun tweet -> tweet.CreatedBy)

        let flowCoordinates =
            Flow.Create<ITweet>()
            |> FlowEx.select (fun tweet -> tweet.Coordinates)

        let writeSink = Sink.ForEach<string>(fun text ->
            effect text)

        let graph = GraphDsl.Create(fun buildBlock ->
            let broadcast = buildBlock.Add(Broadcast<ITweet>(2))
            let merge = buildBlock.Add(Merge<string>(2))

            buildBlock.From(broadcast.Out(0))
            |> FlowEx.via (flowCreateBy
            |> FlowEx.throttle 10 (TimeSpan.FromSeconds(1.)) 1 ThrottleMode.Shaping)
            |> FlowEx.via  formatUser
            |> FlowEx.sink (merge.In(0))
            |> ignore
            
            
            buildBlock.From(broadcast.Out(1))
            |> FlowEx.via (flowCoordinates
                        |> FlowEx.throttle 10 (TimeSpan.FromSeconds(1.)) 10 ThrottleMode.Shaping)
            |> FlowEx.via formatCoordinates
            |> FlowEx.sink (merge.In(1))
            |> ignore

            FlowShape<ITweet, string>(broadcast.In, merge.Out))
 
        (tweetSource
        |> FlowEx.where (Predicate(fun (tweet:ITweet) -> not(isNull tweet.Coordinates)))
        ).Via(graph).To(writeSink)
   
module TweetsWithWeather =
  
    let create<'a>(tweetSource:Source<ITweet, 'a>) effect =

        let formatUser =
            Flow.Create<IUser>()
            |> FlowEx.select (Utils.FormatUser)

        let formatCoordinates =
            Flow.Create<ICoordinates>()
            |> FlowEx.select (Utils.FormatCoordinates)

        let formatTemperature =
            Flow.Create<decimal>()
            |> FlowEx.select (Utils.FormatTemperature)

        let createBy = Flow.Create<ITweet>().Select(fun tweet -> tweet.CreatedBy)

        let writeSink =
            Sink.ForEach<string>(fun msg -> effect msg)

       
        let graph = GraphDsl.Create(fun buildBlock ->
            let broadcast = buildBlock.Add(Broadcast<ITweet>(2))
            let merge = buildBlock.Add(Merge<string>(2))

            buildBlock.From(broadcast.Out(0))
            |> via (Flow.Create<ITweet>()
                    |> FlowEx.select (fun tweet -> tweet.CreatedBy)
                    |> FlowEx.throttle 10 (TimeSpan.FromSeconds(1.)) 1 ThrottleMode.Shaping)
            |> via (formatUser)
            |> sink (merge.In(0))
            |> ignore

            buildBlock.From(broadcast.Out(1))
            |> via (Flow.Create<ITweet>()
                    |> FlowEx.select (fun tweet -> tweet.Coordinates)
                    |> FlowEx.buffer 10 OverflowStrategy.DropNew
                    |> FlowEx.throttle 1 (TimeSpan.FromSeconds(1.)) 1 ThrottleMode.Shaping)
                    |> via (Flow.Create<ICoordinates>()
                             |> FlowEx.selectAsync 4
                                (fun c -> Forecast.getWeatherFromFileAsync c))
            |> via (formatTemperature)
            |> sink (merge.In(1))
            |> ignore

            FlowShape<ITweet, string>(broadcast.In, merge.Out))

        tweetSource
        |> FlowEx.where (Predicate(fun (tweet:ITweet) -> not(isNull tweet.Coordinates)))
        |> mat graph
        |> sink writeSink


module TweetsExperiments = 

    let formatTweet(tweet : ITweet) =
        let builder = new StringBuilder()
        builder.AppendLine("---------------------------------------------------------")
            .AppendLine(sprintf "Tweet from NewYork from: %s :" tweet.CreatedBy.Name)
            .AppendLine()
            .AppendLine(tweet.Text)
            .AppendLine("---------------------------------------------------------")
            .AppendLine() |> ignore
        builder.ToString()
            
    let run () =
        let tweetSource = Source.ActorRef<ITweet>(100, OverflowStrategy.DropHead)
        let formatFlow = Flow.Create<ITweet>().Select(new Func<ITweet, string>(formatTweet))
        let writeSink = Sink.ForEach<string>(new Action<string>(Console.WriteLine))
        
        let countauthors = Flow.Create<ITweet>().StatefulSelectMany(new Func<_>(fun () ->
                            let dict = new Dictionary<string, int>()
        
                            let res =
                                fun (tweet : ITweet) ->
                                    let user = tweet.CreatedBy.Name
                                    if dict.ContainsKey user |> not then
                                        dict.Add(user, 0)
                                     
                                    dict.[user] <- dict.[user] + 1
                                    
                                    (sprintf "%d tweets from user %s" dict.[user] user) |> Seq.singleton
        
                            new Func<ITweet,_>(res)))
        
        let notUsed = new Func<NotUsed,_, _>(fun notUsed _ -> notUsed)
        
        let builder =
            new Func<GraphDsl.Builder<NotUsed>, FlowShape<ITweet, string>, SinkShape<string>, _>(fun b count write ->
                let broadcast = b.Add(new Broadcast<ITweet>(2))
                let output = b.From(broadcast.Out(0)).Via(formatFlow)
                b.From(broadcast.Out(1)).Via(count).To(write) |> ignore
                new FlowShape<ITweet, string>(broadcast.In, output.Out))
            
        
        let graph = GraphDsl.Create(countauthors, writeSink, notUsed, builder)
        
        let sys = System.create("Reactive-Tweets") (Configuration.defaultConfig())    
        let mat = sys.Materializer()
        
        // Start Akka.Net stream
        let actor = tweetSource.Via(graph).To(writeSink).Run(mat)
        
        // Start Twitter stream
        Auth.SetCredentials(new TwitterCredentials(Credentials.consumerKey, Credentials.consumerSecret, Credentials.accessToken, Credentials.accessTokenSecret))
        let stream = Stream.CreateSampleStream()
        
        //stream.AddLocation(CenterOfNewYork)
        stream.TweetReceived |> Observable.add(fun arg -> actor.Tell(arg.Tweet)) // push the tweets into the stream
        stream.StartStream()
    
          
    let runFun () =
        let tweetSource = Source.actorRef OverflowStrategy.DropHead 100
        let formatFlow = Flow.empty<ITweet, _> |> Flow.map formatTweet 
        let writeSink = Sink.forEach (fun tweet -> printfn "%s" tweet)
        
        let countauthors =
            Flow.empty<ITweet,NotUsed>
            |> Flow.statefulCollect (fun (state : Dictionary<string, int>) tweet ->           
                    let user = tweet.CreatedBy.Name
                    if state.ContainsKey user |> not then
                        state.Add(user, 0)
                    state.[user] <- state.[user] + 1
                    state, (sprintf "%d tweets from user %s" state.[user] user) |> Seq.singleton) (new Dictionary<string, int>())
      
        let graph = Graph.create2 (fun notUsed _ -> notUsed) (fun b (count : FlowShape<ITweet, string>) (writer : SinkShape<String>) ->
                let broadcast = b.Add(new Broadcast<ITweet>(2))
                let output = b.From(broadcast.Out(0)).Via(formatFlow)            
                b.From(broadcast.Out(1)).Via(count).To(writer) |> ignore
                new FlowShape<ITweet, string>(broadcast.In, output.Out))
                     countauthors writeSink
                     |> Flow.FromGraph
        
            
        let sys = System.create("Reactive-Tweets") (Configuration.defaultConfig())
        let mat = sys.Materializer()
        
        // Start Akka.Net stream
        let actor = tweetSource |> Source.via graph |> Source.toSink writeSink |> Graph.run mat
            
        // Start Twitter stream
        Auth.SetCredentials(new TwitterCredentials(Credentials.consumerKey, Credentials.consumerSecret, Credentials.accessToken, Credentials.accessTokenSecret))
        let stream = Stream.CreateSampleStream()
        
        stream.TweetReceived |> Observable.add(fun arg -> actor <! arg.Tweet)
        stream.StartStream()


