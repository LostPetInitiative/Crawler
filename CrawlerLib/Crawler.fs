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
    processResource: Downloader.DownloadedFileWithMime -> Async<Result<'T,string>>
    download: RemoteResourseDescriptor -> Async<Result<RemoteResourseLookup,string>>
    reportProcessed: RemoteResourseDescriptor * Result<ResourceProcessResult<'T>,string> -> unit
    saveLocal: RemoteResourseDescriptor * RemoteResourseLookup -> Async<Result<unit,string>>
}

type private CrawlerMsg<'T> =
    |   ProcessResource of RemoteResourseDescriptor
    |   ResourceProcessed of RemoteResourseDescriptor*Result<ResourceProcessResult<'T>,string>
    |   ShutdownRequest of AsyncReplyChannel<unit>

type private AgentState = {
    ongoingJobs: int
    shuttingDown: AsyncReplyChannel<unit> option
}

type Agent<'T>(callbacks:CrawlerCallbacks<'T>) =
    let mbProcessor = MailboxProcessor<CrawlerMsg<'T>>.Start(fun inbox ->
        let rec messageLoop (state:AgentState) = async {
            let! nextState = async {
                match! inbox.Receive() with
                |   ProcessResource(resource) ->
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
                                        match success with
                                        |   Downloaded downloaded ->
                                            match! callbacks.processResource downloaded with
                                            |   Error e -> return Error (sprintf "Resource processing error: %s" e)
                                            |   Ok processed -> return Ok(Processed processed)
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
                                                match! callbacks.processResource data with
                                                |   Error _ -> return! downloadAndProcess()
                                                |   Ok local -> return Ok(Processed local)
                                        }
                                    |   None ->
                                        downloadAndProcess()
                            inbox.Post(ResourceProcessed(resource, result))    
                        } |> Async.Start
                        return { state with ongoingJobs = state.ongoingJobs + 1}
                    |   Some _ ->
                        callbacks.reportProcessed(resource, Error("The agent is shutting down and is not supposed to receive more precess request"))
                        return state
                |   ResourceProcessed(cardDescriptor, result) ->
                    callbacks.reportProcessed(cardDescriptor, result)
                    match state.shuttingDown with
                    |   None -> ()
                    |   Some channel -> 
                        if state.ongoingJobs <= 1 then
                            // last job
                            channel.Reply()
                            
                    return {state with ongoingJobs = max 0 (state.ongoingJobs - 1)}
                |   ShutdownRequest(channel)->
                    return {state with shuttingDown = Some(channel)}                    
                }
            return! messageLoop nextState
        }

        let initialState:AgentState = {
            ongoingJobs = 0
            shuttingDown = None
        }

        messageLoop initialState)

    member _.Enqueue(resource: RemoteResourseDescriptor) =
        mbProcessor.Post(ProcessResource resource)

    member _.Shutdown() =
        mbProcessor.PostAndAsyncReply(fun channel -> ShutdownRequest(channel))