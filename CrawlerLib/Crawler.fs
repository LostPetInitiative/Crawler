module Kashtanka.Crawler

type ResourceID = string

type RemoteResourseDescriptor = {
    ID:ResourceID
    url:string
}

type CrawlerCallbacks<'T> = {
    tryGetLocal: RemoteResourseDescriptor -> Async<Result<Downloader.DownloadedFileWithMime option,string>>
    processResource: Downloader.DownloadedFileWithMime -> Async<Result<'T,string>>
    download: RemoteResourseDescriptor -> Async<Downloader.DownloadResult>
    reportProcessed: RemoteResourseDescriptor * Result<'T,string> -> unit
    saveLocal: RemoteResourseDescriptor * Downloader.DownloadedFileWithMime -> Async<Result<unit,string>>
}

type private CrawlerMsg<'T> =
    |   ProcessResource of RemoteResourseDescriptor
    |   ResourceProcessed of RemoteResourseDescriptor*Result<'T,string>
    |   ShutdownRequest of AsyncReplyChannel<unit>

type private AgentState = {
    ongoingJobs: int
    shuttingDown: AsyncReplyChannel<unit> option
}

/// Generic crawler, that first 
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
                                |   Ok downloaded ->
                                    match! callbacks.saveLocal(resource,downloaded) with
                                    |   Ok() ->
                                        return! callbacks.processResource downloaded
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
                                            match! callbacks.processResource localResource with
                                            |   Error _ -> return! downloadAndProcess()
                                            |   Ok local -> return Ok(local)
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