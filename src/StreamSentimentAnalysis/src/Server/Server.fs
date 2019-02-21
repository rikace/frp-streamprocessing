open System
open System.IO
open System.Threading
open Microsoft.AspNetCore.Builder
open FSharp.Control.Tasks.V2
open Giraffe
open Saturn
open Shared
open Akka
open Akka.Streams
open System.Threading.Tasks
open ServerStreams

let publicPath = Path.GetFullPath "../Client/public"
let port = 8085us

let mutable disposable : IDisposable option = None
let mutable tks : CancellationTokenSource option = None

let updateMap next (ctx :  Microsoft.AspNetCore.Http.HttpContext) =
    task {
        let! location = ctx.BindModelAsync<MarkerLocation>()
        let model = {Type = "pinsentiment"; Data = location}
        let z = Thoth.Json.Net.Encode.Auto.toString (2, model)        
        do! WebSockets.sendMessageToSockets z 
        return! json "Created!" next ctx
    }
    
let start (graphType: GraphType) next (ctx :  Microsoft.AspNetCore.Http.HttpContext) =
    task {
        let update =
            (fun (emotion : MarkerLocation) -> async {
                let model = {Type = "pinsentiment"; Data = emotion}
                let jsonData = Thoth.Json.Net.Encode.Auto.toString (2, model)
                do! WebSockets.sendMessageToSockets jsonData |> Async.AwaitTask
                })
        tks <- Some(new CancellationTokenSource())             
        Task.Factory.StartNew((fun () ->
            disposable <- Some (ServerStreams.startStreamingCache graphType)),tks.Value.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default) |> ignore
        return! json "Started!" next ctx
    }
    
let stop next (ctx :  Microsoft.AspNetCore.Http.HttpContext) =
    task {
        tks |> Option.iter(fun c -> c.Cancel())
        disposable |> Option.iter(fun d -> d.Dispose())
        return! json "Stooped!" next ctx
    }        

let webApp =
    router {
        post "/api/update" updateMap
        post "/api/start" (start GraphType.Sync)
        post "/api/startasync" (start GraphType.Async)
        post "/api/startparallel" (start GraphType.Parallel)
        post "/api/stop" stop
    }

let app =
    application {
        url ("http://0.0.0.0:" + port.ToString() + "/")
        use_router webApp
        memory_cache
        use_static publicPath
        use_json_serializer (Thoth.Json.Giraffe.ThothSerializer())
        use_gzip
        app_config (fun ab ->
            ab.UseWebSockets()
                .UseMiddleware<WebSockets.WebSocketMiddleware>())
    }

run app
