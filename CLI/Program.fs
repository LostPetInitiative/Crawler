open System
open System.IO
open System.Net
open FSharp.Data
open CrawlerLib.Pet911ru
open System.Diagnostics

type Mode =
    |   AllDB
    |   NewCards

[<EntryPoint>]
let main argv =
    async {
        Trace.Listeners.Add(new ConsoleTraceListener()) |> ignore

        let dbPath =
            if argv.Length > 1 then
                argv.[1]
            else
                @"./db"

        Trace.TraceInformation(sprintf "DB path is %s" dbPath)
       
        let mode =
            if argv.Length > 0 then
                match argv.[0] with
                |   "all" ->
                    printfn "Operation mode: Downloading whole DB"
                    AllDB
                |   "new" ->
                    printfn "Operation mode: Downloading new pet cards (since last download)"
                    NewCards
                |   _ -> failwith "first CLI argument must be mode: 'all' or 'new'"
            else
                failwith "first CLI argument must be mode: 'all' or 'new'"

        

        let googleApiKey =
            if argv.Length > 2 then
                Some(argv.[2])
            else
                None

        let maxConcurrentCardDownload = 1
        let maxImage911ConcurrentDownload = 1
        let maxImageGoogleConcurrentDownload = 5
        
        match mode with
        |  AllDB->
            let ids =
                //seq {476712 .. 476712} |> Seq.map (fun idx -> [sprintf "rf%d" idx ; sprintf "rl%d" idx])
                //|> Seq.collect id
                 seq {"rl476712"}
                // |> Seq.take 170
            let cardProcessor = PetCardDownloader(maxConcurrentCardDownload, maxImage911ConcurrentDownload, maxImageGoogleConcurrentDownload, dbPath, googleApiKey)
        
        
            ids |> Seq.iter (fun artId -> cardProcessor.Post(ProcessArtId artId))
            do! cardProcessor.PostWithReply(ShutdownPetCardDownloader)
        |   NewCards->
            Trace.WriteLine(sprintf "Detecting latest downloaded card")
            let cardDirs = Directory.GetDirectories(dbPath) |> Seq.map (fun x -> DirectoryInfo(x).Name) |> Seq.filter (fun x -> x.StartsWith("rf") || x.StartsWith("rl"))

            let getHighestPetId (ids:string seq) =
                let idsArray = Array.ofSeq ids
                if Array.length idsArray = 0 then
                    let startId = "rl450544" // hardcoded start point if the DB is empty
                    Trace.TraceWarning(sprintf "The DB is empty. Starting from %s (exclusively)" startId)
                    startId
                else
                    idsArray |> Seq.map (fun x -> (x, System.Int32.Parse(x.Substring(2)))) |> Seq.maxBy snd |> fst

            let highestId = getHighestPetId cardDirs
            Trace.WriteLine(sprintf "Highest ID which is present locally is %s. Starting crawler loop" highestId)

            let rec crawlerLoop latestId =
                //Trace.TraceInformation(sprintf "crawlerLoop %s" latestId)
                async{
                    let! latestId2,successful =
                        async {
                            match! detectNewCardIds(latestId) with
                            |   Some newIds ->
                                if Set.count newIds > 0 then
                                    Trace.TraceInformation(sprintf "The following IDs appeared in the remote system (after ID %s): %A" latestId newIds)
                                    //let newIdsWithLatestWeHave = Set.add latestId newIds
                                    let cardProcessor = PetCardDownloader(maxConcurrentCardDownload, maxImage911ConcurrentDownload, maxImageGoogleConcurrentDownload, dbPath, googleApiKey)
                                    newIds |> Seq.iter (fun artId -> cardProcessor.Post(ProcessArtId artId))
                                    do! cardProcessor.PostWithReply(ShutdownPetCardDownloader)
                                    let latestId = getHighestPetId newIds
                                    Trace.TraceInformation(sprintf "Successfully downloaded new bunch of cards (%d) in total. New latest downloaded ID is %s" (Set.count newIds) latestId)

                                    Trace.TraceInformation(sprintf "Trying to ping processing pipeline")
                                    let jsonBody = sprintf """{ "cardIds": [%s] }""" (String.Join(',', newIds |> Seq.map (fun x -> sprintf "\"%s\"" x)))
                                    try
                                        let! pingResult =
                                            Http.AsyncRequest(
                                                "http://127.0.0.1:5001/",
                                                headers = [
                                                            HttpRequestHeaders.ContentType HttpContentTypes.Json;
                                                            // HttpRequestHeaders.Accept HttpContentTypes.Json;
                                                            // HttpRequestHeaders.UserAgent agentName;
                                                            // HttpRequestHeaders.Origin urlPrefix
                                                            ],
                                                httpMethod = "POST",
                                                body = TextRequest jsonBody, silentHttpErrors = true, timeout = 10000)
                                        match pingResult.StatusCode with
                                        |   201 ->
                                            Trace.TraceInformation(sprintf "Successfully notified processing pipeline about new cards")
                                            ()
                                        |   code ->
                                            Trace.TraceWarning(sprintf "Failed to notify processing pipeline. Http code %d" code)
                                            ()
                                    with
                                    |   :? WebException as ex->
                                        Trace.TraceWarning(sprintf "Failed to notify processing pipeline: %A" ex);
                                    return latestId, true
                                else
                                    Trace.TraceInformation(sprintf "There are no new cards since card %s" latestId)
                                    return latestId, true
                            |   None ->
                                Trace.TraceError(sprintf "Error getting fresh IDs.")
                                return latestId, false
                            }
                    let sleepSec = 
                        if successful then 30*60 else 60
                    Trace.TraceInformation(sprintf "Sleeping for %d seconds" sleepSec)
                    do! Async.Sleep (1000*sleepSec)
                    Trace.TraceInformation(sprintf "Awaking. Checking whether new cards appeared after %s" latestId2)
                    return! (crawlerLoop latestId2)
                }
            do! crawlerLoop highestId

        return 0
    } |> Async.RunSynchronously
