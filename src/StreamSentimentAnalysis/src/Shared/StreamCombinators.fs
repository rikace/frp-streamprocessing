namespace StreamingCombinators

module FlowEx =
    
    open System
    open System.Threading.Tasks
    open Akka
    open Akka.Streams
    open Akka.Streams.Dsl
    
    let inline asyncMap (parallelism: int) (fn: 'u -> Async<'w>) (flow) : Flow<'t, 'w, 'mat> =
        FlowOperations.SelectAsync(flow, parallelism, new Func<_, _>(fn >> Async.StartAsTask))

    let inline asyncMapUnordered (parallelism: int) (fn: 'u -> Async<'w>) (flow) : Flow<'t, 'w, 'mat> =
        FlowOperations.SelectAsyncUnordered(flow, parallelism, new Func<_, _>(fn >> Async.StartAsTask))

    let inline asyncMapUnordered2 (parallelism: int) (fn: 'u -> Task<'w>) (flow) : Flow<'t, 'w, 'mat> =
        FlowOperations.SelectAsyncUnordered(flow, parallelism, new Func<_, _>(fn))
        
    let inline async (flow: Flow<'t, 'u, 'mat>) : Flow<'t, 'u, 'mat> = flow.Async()
        
    let inline select map (flow:Flow<_,_, NotUsed>) =
        flow.Select(new Func<_,_>(map))
    
    let inline where (predicate:Predicate<_>) (source:Source<_,_>) = source.Where(predicate)
    
    let inline throttle (elements:int) (per:TimeSpan) (maximumBurst:int) (mode:ThrottleMode) (flow:Flow<_,_,_>) =
        flow.Throttle(elements, per, maximumBurst, mode)
    
    let inline buffer (size:int) (strategy:OverflowStrategy) (flow:Flow<_,_,_>) = flow.Buffer(size, strategy)
    
    let inline selectAsync (parallelism:int) (asyncMapper:_ -> Task<_>) (flow:Flow<_, _,NotUsed>) =
        flow.SelectAsync(parallelism, new Func<_, Task<_>>(asyncMapper))
    
    let inline via (b:IGraph<FlowShape<_, _>, NotUsed>) (a:GraphDsl.ForwardOps<_, NotUsed>) = a.Via(b)
    
    let inline sink (b:Inlet<_>) (a:GraphDsl.ForwardOps<_, NotUsed>) = a.To(b)
        
    let inline viaGraph (b:IGraph<FlowShape<_, _>, NotUsed>) (a:Source<_, _>) = a.Via(b)
    
    let inline sinkGraph (b:Sink<_, Task>) (a:Source<_, _>) = a.To(b)
    
[<AutoOpen>]
module Combinators =        
    let inline via< ^a, ^b, ^c when ^a : (member Via: ^b -> ^c )> (b: ^b) a =
          (^a : (member Via: ^b -> ^c) (a, b))
    
    let inline sink b (a: ^a) = (^a : (member To: ^b -> ^c) (a, b))
    
    let inline from< ^a, ^b, ^c when ^a : (member From: ^b -> ^c )> (b: ^b) a =
          (^a : (member From: ^b -> ^c) (a, b)) 

