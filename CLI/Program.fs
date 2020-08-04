open System
open System.IO
open CrawlerLib.Pet911ru
open System.Diagnostics

[<EntryPoint>]
let main argv =
    async {
        Trace.Listeners.Add(ConsoleTraceListener()) |> ignore

        let startId = 434884
        let maxConcurrentDownload = 1
        let maxImageConcurrentDownload = 1
        let dbPath = "db"
        let ids = Seq.init startId (fun idx -> let idx = startId - idx in [sprintf "rf%d" idx ; sprintf "rl%d" idx]) |> Seq.collect id
        let cardProcessor = PetCardDownloader(maxConcurrentDownload, maxImageConcurrentDownload, dbPath)
        let finished = cardProcessor.PostWithReply(Start)
        cardProcessor.Post(ExitAfter 5)
        ids |> Seq.iter (fun artId -> cardProcessor.Post(ProcessArtId artId))
        
        
        do! finished
        return 0
    } |> Async.RunSynchronously
