module Kashtanka.Crawler

open Downloader

type ResourceID = string

type RemoteResourseDescriptor = {
    ID:ResourceID
    url:string
}

type ResourceProcessResult<'T> = 
    |   Processed of 'T
    |   Missing

type CrawlerCallbacks<'T> = {
    tryGetLocal: RemoteResourseDescriptor -> Async<Result<RemoteResourseLookup option,string>>
    processDownloaded: Downloader.DownloadedFileWithMime -> Async<Result<'T,string>>
    download: RemoteResourseDescriptor -> Async<Result<RemoteResourseLookup,string>>    
    saveLocal: RemoteResourseDescriptor * RemoteResourseLookup -> Async<Result<unit,string>>
}

type private CrawlerMsg<'T> =
    |   ProcessResource of RemoteResourseDescriptor * AsyncReplyChannel<Result<ResourceProcessResult<'T>,string>>
    |   ResourceProcessed of Result<ResourceProcessResult<'T>,string> * AsyncReplyChannel<Result<ResourceProcessResult<'T>,string>>
    |   ShutdownRequest of AsyncReplyChannel<unit>

type private AgentState = {
    ongoingJobs: int
    shuttingDown: AsyncReplyChannel<unit> option
}

let moduleName = "Crawler"

let traceInfo = Tracing.traceInfo moduleName

type Agent<'T>(callbacks:CrawlerCallbacks<'T>) =
    let mbProcessor = MailboxProcessor<CrawlerMsg<'T>>.Start(fun inbox ->
        let rec messageLoop (state:AgentState) = async {
            let! nextState = async {
                match! inbox.Receive() with
                |   ProcessResource(resource,channel) ->
                    match state.shuttingDown with
                    |   None ->
                        let downloadAndProcess() =
                            async {
                                let! downloadRes = callbacks.download resource
                                match downloadRes with
                                |   Error msg -> return Error msg
                                |   Ok success ->                                    
                                    match! callbacks.saveLocal(resource,success) with
                                    |   Ok() ->
                                        sprintf "Data for %s persisted to disk" resource.ID |> traceInfo
                                        match success with
                                        |   Downloaded downloaded ->
                                            sprintf "Resource \"%s\" successfully downloaded & saved to disk" resource.ID |> traceInfo
                                            match! callbacks.processDownloaded downloaded with
                                            |   Error e -> return Error (sprintf "Resource processing error: %s" e)
                                            |   Ok processed ->
                                                sprintf "Resource \"%s\" successfully processed (parsed/validated)" resource.ID |> traceInfo
                                                return Ok(Processed processed)
                                        |   Absent ->
                                            return Ok(Missing)
                                    |   Error msg -> return Error msg
                            }
                        async {
                            let! localCheckResult = callbacks.tryGetLocal resource
                            let! result =
                                match localCheckResult with
                                |   Error _ -> downloadAndProcess()
                                |   Ok(localCheck) ->
                                    match localCheck with
                                    |   Some(localResource) ->                                        
                                        async {
                                            match localResource with
                                            |   Absent -> return Ok(Missing)
                                            |   Downloaded data ->
                                                sprintf "Local copy of \"%s\" exists. Verifying it"  resource.ID |> traceInfo
                                                match! callbacks.processDownloaded data with
                                                |   Error _ -> return! downloadAndProcess()
                                                |   Ok local -> return Ok(Processed local)
                                        }
                                    |   None ->
                                        downloadAndProcess()                            
                            inbox.Post(ResourceProcessed(result, channel))    
                        } |> Async.Start
                        return { state with ongoingJobs = state.ongoingJobs + 1}
                    |   Some _ ->
                        channel.Reply(Error("The agent is shutting down and is not supposed to receive more precess request"))
                        return state
                |   ResourceProcessed(result, processedChannel) ->
                    processedChannel.Reply(result)                    
                    match state.shuttingDown with
                    |   None ->()
                    |   Some shutdownChannel -> 
                        if state.ongoingJobs <= 1 then
                            // last job
                            shutdownChannel.Reply()
                            
                    return {state with ongoingJobs = max 0 (state.ongoingJobs - 1)}
                |   ShutdownRequest(channel)->
                    if state.ongoingJobs = 0 then
                        channel.Reply()                                            
                    return {state with shuttingDown = Some(channel)}                    
                }
            return! messageLoop nextState
        }

        let initialState:AgentState = {
            ongoingJobs = 0
            shuttingDown = None
        }

        messageLoop initialState)

    member _.Process(resource: RemoteResourseDescriptor) =
        mbProcessor.PostAndAsyncReply(fun channel -> ProcessResource(resource,channel))

    member _.Shutdown() =
        mbProcessor.PostAndAsyncReply(fun channel -> ShutdownRequest(channel))