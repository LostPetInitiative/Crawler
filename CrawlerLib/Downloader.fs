﻿module Kashtanka.Downloader

open System.Threading
open FSharp.Data

let moduleName = "Downloader"
let traceError msg = Tracing.traceError moduleName msg
let traceWarning msg = Tracing.traceWarning moduleName msg
let traceInfo msg = Tracing.traceInfo moduleName msg

// Fibonacci seq
let genFibonachi N =
    let rec fibonacciSeqBuilder built prev twoStepPrev N =
        if N = 0 then built
        else
            let newVal = prev + twoStepPrev
            fibonacciSeqBuilder (newVal::built) newVal prev (N-1)
    fibonacciSeqBuilder [1;1] 1 1 N |> List.rev

let fibonacciArray = Array.ofList <| genFibonachi 100

type DownloadedFile =
    |   Text of string
    |   Binary of byte[]

let downloadedFileToText (file:DownloadedFile) = 
    match file with
    |   Text(t) -> t
    |   Binary(b) -> System.Text.Encoding.Default.GetString(b)

type DownloadedFileWithMime = DownloadedFile*string option 

type RemoteResourseLookup = 
    /// Remote resource exists and successfully acquired
    |   Downloaded of DownloadedFileWithMime
    /// Reliably checked that the remote resource is missing
    |   Absent

type DownloadResult = Result<RemoteResourseLookup,string>


type internal DownloaderMsg =
    |   Enqueue of url:string * AsyncReplyChannel<DownloadResult>
    |   Shutdown of AsyncReplyChannel<unit>
    |   DoDownloadAttempt of url:string * retryIdx:int * AsyncReplyChannel<DownloadResult>
    |   DownloadFinished of DownloadResult * AsyncReplyChannel<DownloadResult>

/// Returns the downloaded file and it's mime type if it is specified in the headers
let httpRequest (userAgent:string) (timeoutMs:int) (url:string) = 
    async {
        if System.String.IsNullOrEmpty(url) then
            return Error("empty url")
        else
            try
                let headers = [
                    HttpRequestHeaders.UserAgent userAgent;
                    ]
                let rec followRedirectionsFetch curUrl redirectsLeft =
                    async {
                        if redirectsLeft = 0 then return Error(sprintf "Too many redirects for url %s" url)
                        else
                            let! response = Http.AsyncRequest(curUrl,
                                                                headers = headers,
                                                                httpMethod = "GET", silentHttpErrors = true,
                                                                timeout = timeoutMs,
                                                                customizeHttpRequest = fun r -> r.MaximumAutomaticRedirections <- 5; r.AllowAutoRedirect<-true; r)                                                    
                            // for some reason auto redirection following does not work.
                            match response.StatusCode with
                            |   HttpStatusCodes.MovedPermanently                            
                            |   HttpStatusCodes.Found                            
                            |   HttpStatusCodes.PermanentRedirect
                            |   HttpStatusCodes.TemporaryRedirect ->
                                match response.Headers.TryGetValue(HttpResponseHeaders.Location) with
                                |   true, location ->
                                    if location <> curUrl then
                                        sprintf "handling redirect to %s" location |> traceWarning 
                                        return! followRedirectionsFetch location (redirectsLeft - 1)
                                    else
                                        return Error ("Infinite redirection")
                                |   false, _ -> return Error ("Got redirect http response code, but without location header")
                            |   _ -> return Ok response
                    }
                        
                match! followRedirectionsFetch url 5 with
                |   Error er -> return Error er
                |   Ok response ->
                    match response.StatusCode with
                    |   HttpStatusCodes.OK ->
                        let content:DownloadedFile =
                            match response.Body with
                            |   HttpResponseBody.Text text -> Text text
                            |   HttpResponseBody.Binary bin -> Binary bin
                        let contentType = Map.tryFind HttpResponseHeaders.ContentType response.Headers
                        return Ok(Downloaded(content,contentType))
                    |   HttpStatusCodes.NotFound ->
                        return Ok(Absent)
                    |   _ ->
                        let errMsg = sprintf "Got not successful HTTP code: %d. %O" response.StatusCode response.Body
                        return Error errMsg
            with
            |   we ->
                let errMsg = we.ToString()
                traceError(errMsg)
                return Error errMsg
    }

type DownloaderSettings = {
    /// The delays will be Fibonacci sequence multiplied by the `delayUnitMs` milliseconds
    delayUnitMs:int
    maxPermittedDelayMs: int
}

let defaultDownloaderSettings: DownloaderSettings = {
    delayUnitMs = 100;
    maxPermittedDelayMs = 180000;
}

type AgentState = {
    unfinishedJobs: int
    shutdownChannel: AsyncReplyChannel<unit> option
}

type Agent(concurrentDownloads:int, settings:DownloaderSettings, fetch: (string -> Async<DownloadResult>)) =
    let semaphore = new SemaphoreSlim(concurrentDownloads)
    let mbProcessor = MailboxProcessor<DownloaderMsg>.Start(fun inbox ->
        let rec messageLoop state = async {
            let! msg = inbox.Receive()
            match msg with
            |   Enqueue(url,resultChannel) ->
                traceInfo(sprintf "Queuing download %s" url)
                inbox.Post(DoDownloadAttempt(url,0,resultChannel))
                return! messageLoop {state with unfinishedJobs = state.unfinishedJobs + 1}
            |   DoDownloadAttempt(url,retryIdx,resultChannel) ->
                async {
                    do! semaphore.WaitAsync() |> Async.AwaitTask
                    traceInfo(sprintf "Downloading %s. Attempt #%d" url (retryIdx+1))
                    let! downloadRes =  fetch url
                    semaphore.Release() |> ignore
                    match downloadRes with
                    |   Error(errMsg) ->
                        let delayMs = fibonacciArray.[min retryIdx (fibonacciArray.Length-1)] * settings.delayUnitMs
                        if delayMs <= settings.maxPermittedDelayMs then
                            traceWarning (sprintf "Downloaded \"%s\" failed with \"%s\". Attempt #%d. Retry after %.3f sec" url errMsg (retryIdx+1) ((float)delayMs*0.001))
                            do! Async.Sleep delayMs
                            inbox.Post(DoDownloadAttempt(url,retryIdx+1,resultChannel))
                        else
                            traceError (sprintf "Download \"%s\" failed with \"%s\". Next attempt delay %d is more than permitted %d" url errMsg delayMs settings.maxPermittedDelayMs)
                            inbox.Post(DownloadFinished(Error(errMsg),resultChannel))
                    |   Ok(lookup) ->
                        match lookup with
                        |   Absent ->
                                traceInfo(sprintf "Reliably checked that remote resource does not exists: %s" url)
                                inbox.Post(DownloadFinished(Ok(Absent),resultChannel))
                        |   Downloaded(file,mimeType) ->
                            let size =
                                match file with
                                |   Text t -> t.Length
                                |   Binary b -> b.Length
                            traceInfo(sprintf "Downloaded %s. Size %db. Type %A." url size mimeType)
                            inbox.Post(DownloadFinished(Ok(Downloaded(file,mimeType)),resultChannel))
                } |> Async.Start
                return! messageLoop state
            |   DownloadFinished(result,resultChannel) ->
                resultChannel.Reply(result)
                let unfinishedJobs = state.unfinishedJobs - 1
                match state.shutdownChannel with
                |   None -> return! messageLoop { state with unfinishedJobs = unfinishedJobs }
                |   Some(shutdownChannel) -> 
                    if state.unfinishedJobs = 0 then
                        traceInfo("All jobs are processed. Downloader has shut down.")
                        shutdownChannel.Reply()
                    else
                        traceInfo(sprintf "Graceful shutting down downloader (%d downloads still in the queue)" state.unfinishedJobs)
                        return! messageLoop { state with unfinishedJobs = unfinishedJobs }
            |   Shutdown(channel) ->
                if state.unfinishedJobs = 0 then
                    traceInfo("All jobs are processed. Downloader has shut down.")
                    channel.Reply()
                else
                    traceInfo(sprintf "Graceful shutting down downloader (%d downloads still in the queue)" state.unfinishedJobs)
                return! messageLoop {state with shutdownChannel = Some(channel)}
            }
        let initialState = {
            unfinishedJobs = 0
            shutdownChannel = None
        }
            
        messageLoop initialState)

    member _.Download(url) =
        mbProcessor.PostAndAsyncReply(fun resultChan -> Enqueue(url,resultChan))

    member _.Shutdown() =
        mbProcessor.PostAndAsyncReply(fun r -> Shutdown(r))