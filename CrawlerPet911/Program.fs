open Argu

open Kashtanka

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
            


[<EntryPoint>]
let main argv =
    System.Diagnostics.Trace.Listeners.Add(new System.Diagnostics.ConsoleTraceListener()) |> ignore

    let programName = System.AppDomain.CurrentDomain.FriendlyName
    let parser = ArgumentParser.Create<CLIArgs>(programName = programName)
    try
        let usage = parser.PrintUsage(programName = programName)
        let parsed = parser.ParseCommandLine(inputs = argv, raiseOnUsage = true)
        if parsed.IsUsageRequested then
            sprintf "%s" usage |> traceInfo
            0
        else
            let dbDir = parsed.GetResult Dir
            sprintf "Data directory is %s" dbDir |> traceInfo
            if parsed.Contains Range then
                let (firstCardID,lastCardID) = parsed.GetResult Range
                sprintf "Fetching range from %d to %d" firstCardID lastCardID |> traceInfo
                0
            else     
                sprintf "Subcommand missing.\n%s" usage |> traceError
                1
    with
    |   :? ArguException as e ->
        printfn "%s" e.Message // argu exception is parse exception. So don't print stack trace. We print only parse error content.
        2
    |   e ->
        printfn "%s" (e.ToString()) // In all other exception types we print the exception with stack trace.
        3
          
