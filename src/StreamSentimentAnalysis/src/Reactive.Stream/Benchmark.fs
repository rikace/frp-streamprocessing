module Benchmark 

    /// Do countN repetitions of the function f and print the
    /// time elapsed, number of GCs and change in total memory
    let timeTask countN label f  =
    
        let stopwatch = System.Diagnostics.Stopwatch()
    
        // do a full GC at the start but NOT thereafter
        // allow garbage to collect for each iteration
        System.GC.Collect()
        printfn "Started"
    
        let getGcStats() =
            let gen0 = System.GC.CollectionCount(0)
            let gen1 = System.GC.CollectionCount(1)
            let gen2 = System.GC.CollectionCount(2)
            let mem = System.GC.GetTotalMemory(false)
            gen0,gen1,gen2,mem
    
    
        printfn "======================="
        printfn "%s" label
        printfn "======================="
        for iteration in [1..countN] do
            let gen0,gen1,gen2,mem = getGcStats()
            stopwatch.Restart()
            f() |> Async.RunSynchronously
            stopwatch.Stop()
            let gen0',gen1',gen2',mem' = getGcStats()
            // convert memory used to K
            let changeInMem = (mem'-mem) / 1000L
            printfn "#%2i elapsed:%6ims gen0:%3i gen1:%3i gen2:%3i mem:%6iK" iteration stopwatch.ElapsedMilliseconds (gen0'-gen0) (gen1'-gen1) (gen2'-gen2) changeInMem
    
    let time countN label f  =
    
        let stopwatch = System.Diagnostics.Stopwatch()
    
        // do a full GC at the start but NOT thereafter
        // allow garbage to collect for each iteration
        System.GC.Collect()
        printfn "Started"
    
        let getGcStats() =
            let gen0 = System.GC.CollectionCount(0)
            let gen1 = System.GC.CollectionCount(1)
            let gen2 = System.GC.CollectionCount(2)
            let mem = System.GC.GetTotalMemory(false)
            gen0,gen1,gen2,mem
    
    
        printfn "======================="
        printfn "%s" label
        printfn "======================="
        for iteration in [1..countN] do
            let gen0,gen1,gen2,mem = getGcStats()
            stopwatch.Restart()
            f()
            stopwatch.Stop()
            let gen0',gen1',gen2',mem' = getGcStats()
            // convert memory used to K
            let changeInMem = (mem'-mem) / 1000L
            printfn "#%2i elapsed:%6ims gen0:%3i gen1:%3i gen2:%3i mem:%6iK" iteration stopwatch.ElapsedMilliseconds (gen0'-gen0) (gen1'-gen1) (gen2'-gen2) changeInMem
    
    
    
