open System
open AgentWebCrawler.SyncWebCrawler
//open ParallelAgentWebCrawler.ParallelWebCrawler

[<EntryPoint>]
let main argv =
    
    let agent = new WebCrawler(8)
    agent.Submit "https://www.google.com"
    
    Console.ReadLine() |> ignore
    
    agent.Dispose()
    0
