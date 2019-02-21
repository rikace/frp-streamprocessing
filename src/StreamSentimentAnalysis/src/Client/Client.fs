module Client

open Elmish
open Elmish.React
open Fable.Helpers.React
open Fable.Helpers.React.Props
open Fable.PowerPack.Fetch
open Fable
open Shared
open Fulma
open System
open Fable.Import.JS
open Helpers
open Fable.Core.JsInterop
open Fable.Helpers.ReactGoogleMaps
open Fable.Helpers.ReactGoogleMaps.Props

module R = Fable.Helpers.React
module P = Fable.Helpers.React.Props
module M = Fable.Helpers.GoogleMaps

let defaultCenter:Fable.Import.GoogleMaps.LatLngLiteral = Fable.Helpers.GoogleMaps.Literal.createLatLng 40.6892494 -74.0445004

let initMarker index (location : MarkerLocation)=
   marker [
           MarkerProperties.Key (sprintf "pin-%d" index)
           MarkerProperties.Position !^ (Fable.Helpers.GoogleMaps.Literal.createLatLng location.Lat location.Lng)
           MarkerProperties.Icon (sprintf "images/%s-dot.png" location.Color)
           MarkerProperties.Title location.Title] [] 

let getMap markers =
   googleMap [ 
       MapProperties.ApiKey "AIzaSyA5n4u9OAsIPCwzXh5FMrD9QeIqAL0-HVg"
       MapProperties.MapLoadingContainer "maploadercontainer"
       MapProperties.MapContainer "mapcontainer"
       MapProperties.DefaultZoom 2
       MapProperties.DefaultCenter !^ defaultCenter
       MapProperties.Center !^ defaultCenter 
       MapProperties.Markers markers
       ]


type Model = {
    Pins : Import.React.ReactElement list
}

type Msg =
    | LoadSentimentPin of MarkerLocation
    | LoadSentimentPins of MarkerLocation[]
    | Initalized
    | Failed
    | Reset
    | Stop
    | Start
    | StartAsync
    | StartParallel

let initialModel () = { Pins = [] }      
    
let init() : Model * Cmd<Msg> =
      
    initialModel(), Cmd.none
     
let button color txt onClick =
    Button.button [Button.IsFullWidth
                   Button.Color color
                   Button.CustomClass ""
                   Button.OnClick onClick] [ str txt]
    

let update (msg : Msg) (currentModel : Model) : Model * Cmd<Msg> =
    match msg with
    | Reset -> { Pins = [] }, Cmd.none
    | Stop ->
        let cmd =
            Cmd.ofPromise (fun _ -> postRecord "/api/stop" 0 []) () (fun _ -> Initalized) (fun _ -> Failed)
        init()        
    | Start ->
        let cmd =
            Cmd.ofPromise (fun _ -> postRecord "/api/start" 1 []) () (fun _ -> Initalized) (fun _ -> Failed)
        currentModel, cmd
    | StartAsync ->
        let cmd =
            Cmd.ofPromise (fun _ -> postRecord "/api/startasync" 2 []) () (fun _ -> Initalized) (fun _ -> Failed)
        currentModel, cmd
    | StartParallel ->
        let cmd =
            Cmd.ofPromise (fun _ -> postRecord "/api/startparallel" 3 []) () (fun _ -> Initalized) (fun _ -> Failed)
        currentModel, cmd        
    | Initalized -> initialModel(), Cmd.none
    | Failed -> currentModel, Cmd.none
    | LoadSentimentPins ds ->
       let data =
           printfn "received ### %d" ds.Length
           let newPins = ds |> Seq.map(fun d -> initMarker (currentModel.Pins.Length + 1) d) |> Seq.toList
           newPins |> List.append currentModel.Pins           
       {currentModel with Pins = data }, Cmd.none
    | LoadSentimentPin d -> 
       let data =
           let newPin = initMarker (currentModel.Pins.Length + 1) d
           newPin::currentModel.Pins
       {currentModel with Pins = data }, Cmd.none
            
let view (model : Model) (dispatch : Msg -> unit) =
   div [] 
       [Navbar.navbar [Navbar.Color IColor.IsDark] [Navbar.Item.div [] [
           Message.message [ Message.Color IColor.IsDark ] [ Message.header [ ]  [
               Heading.h2 [ Heading.CustomClass "has-text-white" ] [str "Streaming"]
               Columns.columns [ ]
                       [ Columns.columns [ ]
                           [ Column.column [ ] [   ]
                             Column.column [ ]
                               [ Notification.notification [ Notification.Color IsDark ]
                                   [  button IsInfo "Start" (fun _ -> dispatch Start) ] ]
                             Column.column [ ]
                               [ Notification.notification [ Notification.Color IsDark ]
                                   [ button IsInfo "Start Async" (fun _ -> dispatch StartAsync) ] ]
                              ]                           
                         Columns.columns [ ]
                           [ Column.column [ ]
                               [ Notification.notification [ Notification.Color IsDark ]
                                   [ button IsInfo "Start Async Parallel" (fun _ -> dispatch StartParallel) ] ]
                             Column.column [ ]
                               [ Notification.notification [ Notification.Color IsDark ]
                                   [ button IsWarning "Stop" (fun _ -> dispatch Stop) ] ]
                              ] ]
           ]    ]
        ]     ]        
        Container.container [] [Content.content [] [  getMap model.Pins ]]
     ]

let start initial =
    let sub dispatch =
        let socket = Fable.Import.Browser.WebSocket.Create("ws://localhost:8085/ws")
        socket.addEventListener_open (fun _ -> console.log "Socket connected")
        socket.addEventListener_message (fun msg -> 
            let data = msg.data |> unbox<string>
            let pointRes = Thoth.Json.Decode.Auto.fromString<Msg<obj>> data
            match pointRes with
            | Error msg -> console.log ("Socket msg failed:", msg)
            | Ok msg when msg.Type = "pinsentiment" -> 
                msg.Data
                |> unbox<MarkerLocation>
                |> LoadSentimentPin
                |> dispatch
            | Ok msg when msg.Type = "pinsentiments" ->
                printfn "received pinsentiments ### %O" msg.Data
                msg.Data
                |> unbox<MarkerLocation[]>
                |> LoadSentimentPins
                |> dispatch                
            | _ -> failwith "not supported socket registration")
        socket.addEventListener_close (fun _ -> console.log "Socket closed")
    Cmd.ofSub sub
    
    
#if DEBUG
open Elmish.Debug
open Elmish.HMR
#endif


Program.mkProgram init update view
|> Program.withSubscription start
#if DEBUG
|> Program.withConsoleTrace
|> Program.withHMR
#endif

|> Program.withReact "elmish-app"
// #if DEBUG
// |> Program.withDebugger
// #endif
|> Program.run
