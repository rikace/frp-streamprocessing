module WebSockets

open System
open System.Text
open System.Threading
open System.Threading.Tasks
open System.Net.WebSockets
open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.V2

type SocketRegistry private () =
    static let sockets = lazy (System.Collections.Concurrent.ConcurrentBag<WebSocket>())
    static member Items = sockets.Value
    static member Add (socket:WebSocket) = sockets.Value.Add socket
    static member Remove (socket:WebSocket) = sockets.Value.TryTake (ref socket)

let private removeSocket sockets socket =
    sockets
    |> List.choose (fun s -> 
           if s <> socket then Some s
           else None)

let private sendMessage =
    fun (socket : WebSocket) (message : string) -> 
        task {
            let buffer = Encoding.UTF8.GetBytes(message)
            let segment = new ArraySegment<byte>(buffer)
            if socket.State = WebSocketState.Open then 
                do! socket.SendAsync(segment, WebSocketMessageType.Text, true, CancellationToken.None)
            else SocketRegistry.Remove socket |> ignore
        }

let sendMessageToSockets =
    fun message -> 
        task {
            for socket in SocketRegistry.Items do
                try 
                    do! sendMessage socket message
                with _ -> SocketRegistry.Remove socket |> ignore 
        }

type WebSocketMiddleware(next : RequestDelegate) =
    member __.Invoke(ctx : HttpContext) =
        async {
            if ctx.Request.Path = PathString("/ws") then 
                match ctx.WebSockets.IsWebSocketRequest with
                | true -> 
                    let! webSocket = ctx.WebSockets.AcceptWebSocketAsync() |> Async.AwaitTask
                    SocketRegistry.Add webSocket
                    let buffer : byte [] = Array.zeroCreate 4096
                    let! echo = webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None) 
                                |> Async.AwaitTask
                    ()
                | false -> ctx.Response.StatusCode <- 400
            else do! next.Invoke(ctx) |> (Async.AwaitIAsyncResult >> Async.Ignore)
        }
        |> Async.StartAsTask :> Task
