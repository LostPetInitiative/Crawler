open Argu

open Kashtanka
open Kashtanka.CrawlerPet911
open Kashtanka.Crawler
open Kashtanka.pet911.Utils
open Kashtanka.Common

let moduleName = "Pet911CLI"
let traceError msg = Tracing.traceError moduleName msg
let traceWarning msg = Tracing.traceWarning moduleName msg
let traceInfo msg = Tracing.traceInfo moduleName msg


type RangeArgs = 
    |   FirstCard of cardID: int
    |   LastCard of cardID: int
    interface IArgParserTemplate with
        member x.Usage = 
            match x with
            |   FirstCard _ -> "ID of the first card to obtain"
            |   LastCard _ -> "ID of the last card to obtain"


type CLIArgs = 
    |   [<AltCommandLine("-d") ; Mandatory>]Dir of path:string
    |   [<CliPrefix(CliPrefix.None)>]Range of firstCardID:int * lastCardID:int
    interface IArgParserTemplate with
        member x.Usage =
            match x with
            |   Dir _ -> "Directory of the stored data"
            |   Range _ -> "Process a fixed specified range of cards (first,last)"

let userAgent = "KashtankaCrawler/2.0.0-alpha"

[<EntryPoint>]
let main argv =
    System.Diagnostics.Trace.Listeners.Add(new System.Diagnostics.ConsoleTraceListener()) |> ignore

    printfn "привет"

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
          
