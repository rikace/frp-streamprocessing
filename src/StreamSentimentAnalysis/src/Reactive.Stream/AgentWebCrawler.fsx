open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.IO
open System.Net
open System.Text.RegularExpressions

module AgentWebCrawling =
        
    module Helpers =
    
        type Message =
            | Done
            | Mailbox of MailboxProcessor<Message>
            | Stop
            | Url of string option
            | Start of AsyncReplyChannel<unit>
    
        // Gates the number of crawling agents.
        [<Literal>]
        let Gate = 5
    
        // Extracts links from HTML.
        let extractLinks html =
            let pattern1 = "(?i)href\\s*=\\s*(\"|\')/?((?!#.*|/\B|" + 
                           "mailto:|location\.|javascript:)[^\"\']+)(\"|\')"
            let pattern2 = "(?i)^https?"
     
            let links =
                [
                    for x in Regex(pattern1).Matches(html) do
                        yield x.Groups.[2].Value
                ]
                |> List.filter (fun x -> Regex(pattern2).IsMatch(x))
            links
        
        // Fetches a Web page.
        let fetch (url : string) =
            try
                let req = WebRequest.Create(url) :?> HttpWebRequest
                req.UserAgent <- "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)"
                req.Timeout <- 5000
                use resp = req.GetResponse()
                let content = resp.ContentType
                let isHtml = Regex("html").IsMatch(content)
                match isHtml with
                | true -> use stream = resp.GetResponseStream()
                          use reader = new StreamReader(stream)
                          let html = reader.ReadToEnd()
                          Some html
                | false -> None
            with
            | _ -> None
        
        let collectLinks url =
            let html = fetch url
            match html with
            | Some x -> extractLinks x
            | None -> []
    
    open Helpers
    
    let crawl url limit = 
        // Concurrent queue for saving collected urls.
        let q = ConcurrentQueue<string>()
        
        // Holds crawled URLs.
        let set = HashSet<string>()
    
        // Creates a mailbox that synchronizes printing to the console (so 
        // that two calls to 'printfn' do not interleave when printing)
        let printer = 
            MailboxProcessor.Start(fun x -> async {
              while true do 
                let! str = x.Receive()
                printfn "%s" str })
        // Hides standard 'printfn' function (formats the string using 
        // 'kprintf' and then posts the result to the printer agent.
        let printfn fmt = 
            Printf.kprintf printer.Post fmt
    
        let supervisor =
            MailboxProcessor.Start(fun x -> async {
                // The agent expects to receive 'Start' message first - the message
                // carries a reply channel that is used to notify the caller
                // when the agent completes crawling.
                let! start = x.Receive()
                let repl =
                  match start with
                  | Start repl -> repl
                  | _ -> failwith "Expected Start message!"
    
                let rec loop run =
                    async {
                        let! msg = x.Receive()
                        match msg with
                        | Mailbox(mailbox) ->
                            // limit
                            let count = set.Count
                            if count < limit - 1 && run then 
                                let url = q.TryDequeue()
                                match url with
                                | true, str -> if not (set.Contains str) then
                                                    let set'= set.Add str
                                                    mailbox.Post <| Url(Some str)
                                                    return! loop run
                                                else
                                                    mailbox.Post <| Url None
                                                    return! loop run
    
                                | _ -> mailbox.Post <| Url None
                                       return! loop run
                            else
                                mailbox.Post Stop
                                return! loop run
                        | Stop -> return! loop false
                        | Start _ -> failwith "Unexpected start message!"
                        | Url _ -> failwith "Unexpected URL message!"
                        | Done -> printfn "Supervisor is done."
                                  (x :> IDisposable).Dispose()
                                  // Notify the caller that the agent has completed
                                  repl.Reply(())
                    }
                do! loop true })
    
        
        let urlCollector =
            MailboxProcessor.Start(fun y ->
                let rec loop count =
                    async {
                        let! msg = y.TryReceive(6000)
                        match msg with
                        | Some message ->
                            match message with
                            | Url u ->
                                match u with
                                | Some url -> q.Enqueue url
                                              return! loop count
                                | None -> return! loop count
                            | _ ->
                                match count with
                                | Gate -> supervisor.Post Done
                                          (y :> IDisposable).Dispose()
                                          printfn "URL collector is done."
                                | _ -> return! loop (count + 1)
                        | None -> supervisor.Post Stop
                                  return! loop count
                    }
                loop 1)
        
        /// Initializes a crawling agent.
        let crawler id =
            MailboxProcessor.Start(fun inbox ->
                let rec loop() =
                    async {
                        let! msg = inbox.Receive()
                        match msg with
                        | Url x ->
                            match x with
                            | Some url -> 
                                    let links = collectLinks url
                                    printfn "%s crawled by agent %d." url id
                                    for link in links do
                                        urlCollector.Post <| Url (Some link)
                                    supervisor.Post(Mailbox(inbox))
                                    return! loop()
                            | None -> supervisor.Post(Mailbox(inbox))
                                      return! loop()
                        | _ -> urlCollector.Post Done
                               printfn "Agent %d is done." id
                               (inbox :> IDisposable).Dispose()
                        }
                loop())
    
        // Send 'Start' message to the main agent. The result
        // is asynchronous workflow that will complete when the
        // agent crawling completes
        let result = supervisor.PostAndAsyncReply(Start)
        // Spawn the crawlers.
        let crawlers = 
            [
                for i in 1 .. Gate do
                    yield crawler i
            ]
        
        // Post the first messages.
        crawlers.Head.Post <| Url (Some url)
        crawlers.Tail |> List.iter (fun ag -> ag.Post <| Url None) 
        result
    
    // Example:
    crawl "http://news.google.com" 25
    |> Async.RunSynchronously
    
module ParallelAgentWebCrawling =
    
    open System
    
    // Prelude - just a simple immutable queue.
    type Queue<'a>(xs : 'a list, rxs : 'a list) =
        new() = Queue([], [])
        static member Empty() = new Queue<'a>([], [])
    
        member q.IsEmpty = (List.isEmpty xs) && (List.isEmpty rxs)
        member q.Enqueue(x) = Queue(xs, x::rxs)
        member q.TryTake() =
            if q.IsEmpty then None, q
            else
                match xs with
                | [] -> (Queue(List.rev rxs,[])).TryTake()
                | y::ys -> Some(y), (Queue(ys, rxs))
    
    type Url = Url of string
    
    type ContentHash = ContentHash of byte[]
    
    type CrawlSession = CrawlSession of Guid
    
    type Crawl =
        {
            Session : CrawlSession
            Url : Url
            Headers : (string * string) list
            Content : byte[]
        }
    
    type Config =
        {
            StoreCrawl : Crawl -> Async<unit>
            GetNextPages : Crawl -> Url list
            DegreeOfParallelism : int
        }
    
    type Dependencies =
        {
            Fetch : Url -> Async<Crawl>
            Config : Config
        }
    
    type WorkerMessage =
        | Fetch of Url
    
    type SupervisorMessage =
        | Start of MailboxProcessor<SupervisorMessage> * Url * AsyncReplyChannel<Url Set>
        | FetchCompleted of Url * (Url list)
    
    type SupervisorProgress =
        {
            Supervisor : MailboxProcessor<SupervisorMessage>
            ReplyChannel : AsyncReplyChannel<Url Set>
            Workers : MailboxProcessor<WorkerMessage> list
            PendingUrls : Url Queue
            Completed : Url Set
            Dispatched : int
        }
    
    type SupervisorStatus =
        | NotStarted
        | Running of SupervisorProgress
        | Finished
    
    let startWorker dependencies (supervisor : MailboxProcessor<SupervisorMessage>) =
        MailboxProcessor.Start(fun inbox ->
            let rec loop () =
                async {
                    let! Fetch(url) = inbox.Receive()
                    let! crawl = dependencies.Fetch url
                    do! dependencies.Config.StoreCrawl(crawl)
                    let nextUrls = dependencies.Config.GetNextPages(crawl)
                    supervisor.Post(FetchCompleted(url, nextUrls))
                    return! loop()
                }
            loop())
    
    let rec dispatch dependencies progress =
        match progress.PendingUrls.TryTake() with
        | None, _ -> progress
        | Some url, queue ->
            match progress.Workers |> List.tryFind (fun worker -> worker.CurrentQueueLength = 0) with
            | Some idleWorker ->
                idleWorker.Post(Fetch url)
                dispatch dependencies { progress with
                                            PendingUrls = queue
                                            Dispatched = progress.Dispatched + 1 }
            | None when progress.Workers.Length < dependencies.Config.DegreeOfParallelism ->
                let newWorker = startWorker dependencies (progress.Supervisor)
                dispatch dependencies { progress with Workers = newWorker :: progress.Workers }
            | _ ->
                progress
    
    let enqueueUrls urls progress =
        let pending = progress.PendingUrls |> List.foldBack(fun url pending -> pending.Enqueue(url)) urls
        { progress with PendingUrls = pending }
    
    let complete url progress =
        { progress with
            Completed = progress.Completed.Add(url)
            Dispatched = progress.Dispatched - 1 }
    
    let start supervisor replyChannel =
        {
            Supervisor = supervisor
            ReplyChannel = replyChannel
            Workers = []
            PendingUrls = Queue.Empty()
            Completed = Set.empty
            Dispatched = 0
        }
    
    let handleStart dependencies supervisor url replyChannel =
        start supervisor replyChannel
        |> enqueueUrls [url]
        |> dispatch dependencies
        |> Running
    
    let handleFetchCompleted dependencies url nextUrls progress =
        let progress =
            progress
            |> complete url
            |> enqueueUrls nextUrls
            |> dispatch dependencies
        if progress.PendingUrls.IsEmpty && progress.Dispatched = 0 then
            progress.ReplyChannel.Reply(progress.Completed)
            Finished
        else
            Running progress
    
    let handleSupervisorMessage dependencies message state =
        match message with
        | Start (supervisor, url, replyChannel) ->
            match state with
            | NotStarted ->
                handleStart dependencies supervisor url replyChannel
            | _ -> failwith "Invalid state: Can't be started more than once."
        | FetchCompleted(url, nextUrls) ->
            match state with
            | Running progress ->
                handleFetchCompleted dependencies url nextUrls progress
            | _ -> failwith "Invalid state - can't complete fetch before starting."
    
    let fetchRecursiveInternal dependencies startUrl =
        let supervisor = MailboxProcessor<SupervisorMessage>.Start(fun inbox ->
            let rec loop state =
                async {
                    let! message = inbox.Receive()
                    match state |> handleSupervisorMessage dependencies message with
                    | Finished -> return ()
                    | newState -> return! loop newState
                }
            loop NotStarted)
        supervisor.PostAndAsyncReply(fun replyChannel -> Start(supervisor, startUrl, replyChannel))
    
    let fetchRecursive (config : Config) (startUrl : Url) : Async<Url Set> =
        // TODO: write a real fetch method.
        let fetch url = async { return failwith "Not Implemented" }
        let dependencies = { Config = config; Fetch = fetch }
        fetchRecursiveInternal dependencies startUrl
    
    // Simple test harness using mocked internal dependencies.
    let runTest () =
        let startUrl = Url "http://test.com"
        let childPages = [ Url "http://test.com/1"; Url "http://test.com/2" ]
        let fetch url =
            async {
                return
                    {
                        Url = url
                        Session = CrawlSession(System.Guid.NewGuid())
                        Headers = []
                        Content = [||]
                    }
            }
        let dependencies =
            {
                Fetch = fetch
                Config =
                    {
                        DegreeOfParallelism = 2
                        GetNextPages =
                            function
                            | crawl when crawl.Url = startUrl -> childPages
                            | _ -> []
                        StoreCrawl = fun _ -> async { return () }
                    }
            }
        fetchRecursiveInternal dependencies startUrl |> Async.RunSynchronously
        
      