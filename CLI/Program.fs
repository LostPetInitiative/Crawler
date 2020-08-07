open System
open System.IO
open CrawlerLib.Pet911ru
open System.Diagnostics

[<EntryPoint>]
let main argv =
    async {
        Trace.Listeners.Add(new ConsoleTraceListener()) |> ignore

        let dbPath =
            if argv.Length > 0 then
                argv.[0]
            else
                @"./db"
        let googleApiKey =
            if argv.Length > 1 then
                Some(argv.[1])
            else
                None

        Trace.WriteLine(sprintf "DB path is %s" dbPath)

        let startId = 434884
        let maxConcurrentCardDownload = 5
        let maxImage911ConcurrentDownload = 1
        let maxImageGoogleConcurrentDownload = 2
        //let dbPath = @"D:\pet911ru"
        //let dbPath = @"./db"

        //let! res = tryExtractCard "rf434884"

        let ids =
            Seq.init startId (fun idx -> let idx = startId - idx in [sprintf "rf%d" idx ; sprintf "rl%d" idx])
            |> Seq.collect id
            // |> Seq.take 170
        let cardProcessor = PetCardDownloader(maxConcurrentCardDownload, maxImage911ConcurrentDownload, maxImageGoogleConcurrentDownload, dbPath, googleApiKey)
        
        
        ids |> Seq.iter (fun artId -> cardProcessor.Post(ProcessArtId artId))
        do! cardProcessor.PostWithReply(ShutdownPetCardDownloader)
        
        return 0
    } |> Async.RunSynchronously
