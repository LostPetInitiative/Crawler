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

/// How many recent IDs to track
let maxKnownSetCount = 50

type CLIArgs = 
    |   [<AltCommandLine("-d") ; Mandatory>]Dir of path:string
    |   [<CliPrefix(CliPrefix.None)>]Range of firstCardID:int * lastCardID:int
    |   [<CliPrefix(CliPrefix.None)>]NewCards of checkIntervalSec:int * pingPipeline: bool
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
                    let initialKnownIds =
                        System.IO.Directory.EnumerateDirectories(dbDir)
                        |> Seq.map (fun x -> System.IO.Path.GetRelativePath(dbDir,x))
                        |> Seq.filter (fun x -> x.StartsWith("rl") || x.StartsWith("rf"))
                        |> Seq.map (fun x -> System.Int32.Parse(x.Substring(2)))
                        |> Seq.sortDescending
                        |> Seq.truncate maxKnownSetCount // not more than 50 latest ads. A way to workaround paid and deleted ads
                        |> Array.ofSeq
                    printfn "Already known cards: %A" initialKnownIds
                    let rec loop arg =
                        let maxKnownOpt, knownSet = arg
                        async {
                            let! nextIterArg =
                                async {
                                    match! NewCards.getNewCards knownSet downloadResource with
                                    |   Error er ->
                                        sprintf "Error while detecting new cards: %s" er |> traceError
                                        return arg
                                
                                    |   Ok cardIdsAll ->
                                        let cardIds = 
                                            match maxKnownOpt with
                                            |   None -> cardIdsAll
                                            |   Some maxKnown ->
                                                sprintf "Latest known card: %d" maxKnown |> traceInfo
                                                cardIdsAll
                                                |> Set.filter (fun x -> System.Int32.Parse(x.ID.Substring(2)) > maxKnown) // more recent than max known
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
                                        
                                        let successfulNewCards =
                                            let mapper idx r =
                                                match r with
                                                |   Error _ -> None
                                                |   Ok() -> Some(cardsIdsArray.[idx],System.Int32.Parse(cardsIdsArray.[idx].ID.Substring(2)))
                                            results
                                            |> Seq.mapi mapper
                                            |> Seq.choose id
                                            |> Seq.sortByDescending snd
                                            |> Seq.toArray
                                        let successfulNewCardsIds = Array.map fst successfulNewCards
                                        let successfulNewCardsNums = Array.map snd successfulNewCards |> Array.sortDescending


                                        // pinging pipeline
                                        if pingPipelineEnabled && successfulNewCardsIds.Length>0 then
                                            match! pingPipeline (successfulNewCardsIds |> Seq.map (fun x -> x.ID)) with
                                            |   Error er ->
                                                sprintf "Failed to ping pipeline: %s" er |> traceError
                                                exit 4
                                            |   Ok() -> traceInfo "Successfully notified processing pipeline" 

                                        let newKnownSet =
                                            successfulNewCardsNums
                                            |> Array.fold (fun s newCard -> Set.add newCard s) (Option.defaultValue (Set.empty) knownSet)
                                            |> Set.toSeq
                                            |> Seq.sortDescending
                                            |> Seq.truncate maxKnownSetCount
                                            |> Set.ofSeq
                                            
                                        return (Array.tryHead successfulNewCardsNums, if Set.isEmpty newKnownSet then None else Some(newKnownSet))
                                    }
                            sprintf "Sleeping %d seconds..." checkIntervalSec |> traceInfo
                            do! Async.Sleep (System.TimeSpan.FromSeconds (float checkIntervalSec))
                            return! loop nextIterArg
                        }
                    do! loop (
                        Array.tryHead initialKnownIds, // max id
                        if initialKnownIds.Length = 0 then None else Some(Set.ofArray initialKnownIds))
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
          
