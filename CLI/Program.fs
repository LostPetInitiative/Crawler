open System
open System.IO
open CrawlerLib.Pet911ru
open System.Diagnostics

[<EntryPoint>]
let main argv =
    async {
        Trace.Listeners.Add(new ConsoleTraceListener()) |> ignore

        let startId = 434884
        let maxConcurrentDownload = 5
        let maxImageConcurrentDownload = 1
        let dbPath = @"D:\pet911ru"

        //let! res = tryExtractCard "rf434884"

        let ids =
            Seq.init startId (fun idx -> let idx = startId - idx in [sprintf "rf%d" idx ; sprintf "rl%d" idx])
            |> Seq.collect id
            // |> Seq.take 170
        let cardProcessor = PetCardDownloader(maxConcurrentDownload, maxImageConcurrentDownload, dbPath)
        
        
        ids |> Seq.iter (fun artId -> cardProcessor.Post(ProcessArtId artId))
        do! cardProcessor.PostWithReply(ShutdownPetCardDownloader)
        
        return 0
    } |> Async.RunSynchronously
