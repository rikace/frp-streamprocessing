namespace Shared

type EmotionType =
     { emotion:int }
       with
         static member zero = { emotion = 0 }
         
         static member consoleColor (x:EmotionType) =
            match x.emotion with
            | 0 | 1 -> System.ConsoleColor.Red
            | 2 -> System.ConsoleColor.Cyan
            | 3 | 4 -> System.ConsoleColor.Green
            | x -> System.ConsoleColor.Yellow
             
         member x.toColor() =
            match x.emotion with
            | 0 | 1 -> "red"
            | 2 -> "green"
            | 3 -> "blue"
            | 4 -> "yellow"
            | x -> failwith (sprintf "Unknown emotion value %d" x)
            
         override this.ToString() =
            match this.emotion with
            | 0 | 1 -> "Unhappy"
            | 2 -> "Indifferent"
            | 3 | 4 -> "Happy"
            | x -> failwith (sprintf "Unknown emotion value %d" x)

type MarkerLocation =
    { Lat : float
      Lng : float
      Title : string
      Temperature : decimal 
      Emotion : EmotionType
      Color : string }
    
type Msg<'a> =
    {  Type : string
       Data : 'a }

module Helpers =
    open System
    
    let either a b = if String.IsNullOrEmpty a then b else a
    
    let normalize (em:float32) =
         // Happy
         //Val : 4 - max : 406.204900  - min : -189.103600
         //Val : 3 - max : 431.193100  - min : -331.100800            
         // INdifferent 
         // Val : 2 - max : 419.557500  - min : -311.066700            
         // Unhappy
         // Val : 1 - max : 430.839700  - min : -451.206700
         // Val : 0 - max : 238.980100  - min : -141.332000
         if em <= 406.204900f && em >= 306.103600f then 4
         elif em <= 306.103600f && em >= 0.f then 3
         elif em <= 0.f && em >= -221.206700f then 2
         elif em <= -221.206700f && em >= -431.100800f then 1
         else 0

    let zeroMarker =
        {
            Lat = 0.
            Lng = 0.
            Title = ""
            Temperature = 0M
            Emotion = EmotionType.zero
            Color = ""
        }