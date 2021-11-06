open Argu

open Kashtanka
open Kashtanka.pet911.Crawler
open Kashtanka.Crawler
open Kashtanka.pet911.Utils
open Kashtanka.Common

let moduleName = "Pet911CLI"
let traceError msg = Tracing.traceError moduleName msg
let traceWarning msg = Tracing.traceWarning moduleName msg
let traceInfo msg = Tracing.traceInfo moduleName msg


type CLIArgs = 
    |   [<AltCommandLine("-d") ; Mandatory>]Dir of path:string
    |   [<CliPrefix(CliPrefix.None)>]Range of firstCardID:int * lastCardID:int
    |   [<CliPrefix(CliPrefix.None)>]NewCards of checkIntervalSet:int * pingPipeline: bool
    interface IArgParserTemplate with
        member x.Usage =
            match x with
            |   Dir _ -> "Directory of the stored data"
            |   Range _ -> "Process a fixed specified range of cards (first,last)"
            |   NewCards _ -> "Detect and download new cards loop (intervals in seconds between checks) (boolean: whether to notify processing pipeline with POST HTTP requests)"

let userAgent = "KashtankaCrawler/2.0.0-alpha"

[<EntryPoint>]
let main argv =
    System.Diagnostics.Trace.Listeners.Add(new System.Diagnostics.ConsoleTraceListener()) |> ignore

    let programName = System.AppDomain.CurrentDomain.FriendlyName
    let parser = ArgumentParser.Create<CLIArgs>(programName = programName)
    try
        async {
            let usage = parser.PrintUsage(programName = programName)
            let parsed = parser.ParseCommandLine(inputs = argv, raiseOnUsage = true)
            if parsed.IsUsageRequested then
                sprintf "%s" usage |> traceInfo
                return 0
            else
                let dbDir = parsed.GetResult Dir
                sprintf "Data directory is %s" dbDir |> traceInfo
                System.IO.Directory.CreateDirectory(dbDir) |> ignore
                                   
                let downloadingAgent =
                    let fetchUrl = Downloader.httpDownload userAgent 60000
                    new Downloader.Agent(1, Downloader.defaultDownloaderSettings, fetchUrl)
                let downloadResource (desc:RemoteResourseDescriptor) = downloadingAgent.Download desc.url

                let! crawler = constructCrawler dbDir downloadResource
                if parsed.Contains Range then
                    let (firstCardID,lastCardID) = parsed.GetResult Range
                    sprintf "Fetching range from %d to %d" firstCardID lastCardID |> traceInfo
                    
                    let! jobs =
                        cardIDsFromRange firstCardID lastCardID
                        |> Seq.map cardIDtoDescriptor
                        |> Seq.map crawler
                        |> Async.Parallel
                    match allResults jobs with
                    |   Error er ->
                        traceError er
                        return 2
                    |   Ok _ ->
                        traceInfo "All jobs are complete"
                        return 0
                elif parsed.Contains NewCards then
                    let checkIntervalSec,pingPipelineEnabled = parsed.GetResult NewCards
                    let failedLogPath = System.IO.Path.Combine(dbDir,"failedCards.log")
                    sprintf "Entering new cards monitoring mode. Poll interval: %d sec" checkIntervalSec |> traceInfo
                    let latest =
                        System.IO.Directory.EnumerateDirectories(dbDir)
                        |> Seq.map (fun x -> System.IO.Path.GetRelativePath(dbDir,x))
                        |> Seq.filter (fun x -> x.StartsWith("rl") || x.StartsWith("rf"))
                        |> Seq.sortByDescending (fun x -> System.Int32.Parse(x.Substring(2)))
                        |> Seq.map cardIDtoDescriptor
                        |> Seq.tryHead
                    let rec loop latestKnown =
                        sprintf "Latest known card: %A" latestKnown |> traceInfo
                        async {
                            let! latestKnown2 =
                                async {
                                    match! NewCards.getNewCards latestKnown downloadResource with
                                    |   Error er ->
                                        sprintf "Error while detecting new cards: %s" er |> traceError
                                        return latestKnown
                                
                                    |   Ok cardIdsAll ->
                                        let cardIds = 
                                            match latestKnown with
                                            |   None -> cardIdsAll
                                            |   Some latestKnownCard ->
                                                let latestNum = System.Int32.Parse(latestKnownCard.ID.Substring(2))
                                                cardIdsAll
                                                |> Set.filter (fun x -> System.Int32.Parse(x.ID.Substring(2)) > latestNum)
                                        sprintf "Detected %d new cards: %A" cardIds.Count (Set.map (fun x -> x.ID) cardIds) |> traceInfo
                                        let cardsIdsArray = Array.ofSeq cardIds
                                        let! results =  cardsIdsArray |> Seq.map crawler |> Async.Parallel

                                        // dumping failed results to disk
                                        let failed =
                                            let mapper idx r =
                                                match r with
                                                |   Error er -> Some (er,cardsIdsArray.[idx].ID)
                                                |   Ok _ -> None
                                            results
                                            |> Seq.mapi mapper
                                            |> Seq.choose id
                                            |> Seq.toArray
                                        failed |> Seq.iter (fun x -> let (msg,cId) = x in traceError(sprintf "Card %s failed: %s" cId msg))
                                        let failedIDs = failed |> Array.map snd
                                        if Array.isEmpty failedIDs then
                                            do! System.IO.File.AppendAllLinesAsync(failedLogPath,failedIDs) |> Async.AwaitTask
                                        
                                        let successfulNewCard =
                                            let mapper idx r =
                                                match r with
                                                |   Error _ -> None
                                                |   Ok() -> Some(cardsIdsArray.[idx],System.Int32.Parse(cardsIdsArray.[idx].ID.Substring(2)))
                                            results
                                            |> Seq.mapi mapper
                                            |> Seq.choose id
                                            |> Seq.sortByDescending snd
                                            |> Seq.map fst
                                            |> Seq.toArray

                                        // pinging pipeline
                                        if pingPipelineEnabled && successfulNewCard.Length>0 then
                                            match! pingPipeline (successfulNewCard |> Seq.map (fun x -> x.ID)) with
                                            |   Error er ->
                                                sprintf "Failed to ping pipeline: %s" er |> traceError
                                                exit 4
                                            |   Ok() -> traceInfo "Successfully notified processing pipeline" 

                                        let newLatest = 
                                            successfulNewCard
                                            |> Seq.tryHead
                                        match newLatest with
                                        |   None -> return latestKnown
                                        |   Some l -> return Some l
                                    }
                            sprintf "Sleeping %d seconds..." checkIntervalSec |> traceInfo
                            do! Async.Sleep (System.TimeSpan.FromSeconds (float checkIntervalSec))
                            return! loop latestKnown2

                        }
                    do! loop latest
                    return 0 // unreachable
                else     
                    sprintf "Subcommand missing.\n%s" usage |> traceError
                    return 1
        } |> Async.RunSynchronously
    with
    |   :? ArguException as e ->
        printfn "%s" e.Message // argu exception is parse exception. So don't print stack trace. We print only parse error content.
        2
    |   e ->
        printfn "%s" (e.ToString()) // In all other exception types we print the exception with stack trace.
        3
          
