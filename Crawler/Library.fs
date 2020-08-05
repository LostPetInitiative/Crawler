namespace CrawlerLib

open Newtonsoft.Json
open FSharp.Data
open System.Diagnostics
open System.Net
open System.IO
open System.Threading

module Pet911ru =
    type CardSearchResult =
        {
            art: string
            url: string
        }

    type CardJson = JsonProvider<"../data/petCard.json">

    let urlPrefix = @"https://pet911.ru"
    let agentName = "LostPetInitiative:Crawler-pet911.ru / 0.1 (https://github.com/LostPetInitiative/Crawler-pet911.ru)"

    let private apiRequest path jsonBody =
        let fullURL = sprintf "%s%s" urlPrefix path
        // Trace.TraceInformation(sprintf "Requesting %s (%s)" fullURL jsonBody)
        Http.AsyncRequest(
            fullURL,
            headers = [
                        HttpRequestHeaders.ContentType HttpContentTypes.Json;
                        //HttpRequestHeaders.Accept HttpContentTypes.Json;
                        HttpRequestHeaders.UserAgent agentName;
                        //HttpRequestHeaders.Origin urlPrefix
                        ],
            httpMethod = "POST",
            body = TextRequest jsonBody, silentHttpErrors = false, timeout = 30000)

    /// Extracts the URL for some cardID (if any)
    let tryFetchCardPageURL cardId = 
        // https://pet911.ru/api/pets/check-pet
        //
        // request:
        //  {
        //      "art": "rf434545"
        //  }
        //
        // found response:
        // {"art":"rf434545","url":"/Чебоксары/найдена/собака/rf434545"}
        //
        // not found response:
        //  { "art": "rl434545" }
        let jsonBody = sprintf """{"art": "%s"}""" cardId
        async {
            let! response = apiRequest "/api/pets/check-pet" jsonBody
            match response.StatusCode with
            |   HttpStatusCodes.OK ->
                match response.Body with
                |   HttpResponseBody.Text responseStr->
                    let responseJson = JsonConvert.DeserializeObject<CardSearchResult>(responseStr)
                    if System.String.IsNullOrEmpty(responseJson.url) then
                        return Some(sprintf "%s/%s" urlPrefix responseJson.url)
                    else
                        return None
                |   HttpResponseBody.Binary _ ->
                    Trace.TraceError(sprintf "Binary body returned when searching for a card by \'%s\' request" cardId)
                    return None
            |   _ ->
                Trace.TraceError(sprintf "Non successful error code (%d) when searching for a card by \'%s\' request" response.StatusCode cardId)
                return None
        }
    
    /// Tries to extract the pet card by art
    let tryExtractCard cardId =
        let jsonBody = sprintf """{"art": "%s"}""" cardId
        // request 
        // { "art": "rf434545" }
        async {
            let! responseOption =
                async {
                    try
                        let! response = apiRequest "/api/view-pet" jsonBody
                        // Trace.TraceInformation(sprintf "Got reply for %s" cardId)
                        return Some response
                    with
                    |   :? WebException as we ->
                        if we.Status = WebExceptionStatus.ProtocolError then
                            //Trace.TraceInformation(sprintf "WE %O for %s" we  cardId);
                            ()
                        else
                            Trace.TraceInformation(sprintf "WE %O for %s status %O" we  cardId we.Status);
                        return None
                }
            match responseOption with
            |   Some response ->
                match response.StatusCode with
                |   HttpStatusCodes.OK ->
                    let contentTypeFound, contentType = response.Headers.TryGetValue HttpResponseHeaders.ContentType
                    if contentTypeFound then
                        if contentType.Contains(@"application/json") then
                            match response.Body with
                            |   HttpResponseBody.Text responseStr ->
                                let responseJson = CardJson.Parse(responseStr)
                                return Some(responseJson)
                            |   HttpResponseBody.Binary _ ->
                                Trace.TraceError(sprintf "Binary body returned when extracting a pet card \'%s\'" cardId)
                                return None
                        else
                            return None
                    else
                        Trace.TraceWarning(sprintf "The response for artId %s does not contain ContentType header" cardId)
                        return None
                |   HttpStatusCodes.NotFound ->
                        return None
                |   _ ->
                    Trace.TraceError(sprintf "Non successful error code (%d) when extracting the pet card \'%s\'" response.StatusCode cardId)
                    return None
            |   None -> return None
        }

    type ImageDownloaderState = {
        ActiveDownloads: int
        ShutdownChannel: AsyncReplyChannel<unit> option
    }


    type ImageDownloaderMsg =
    |   DownloadImage of uri:System.Uri*filename:string
    |   DownloadedImage of uri:System.Uri*filename:string
    |   ShutdownImageDownloader of AsyncReplyChannel<unit>

    type ImageDownloader(maxConcurrentDownloads) =
        let semaphore = new SemaphoreSlim(maxConcurrentDownloads)

        let download url path =
            use client = new WebClient()
            async {
                do! Async.SwitchToThreadPool()
                do! semaphore.WaitAsync() |> Async.AwaitTask
                try
                    do! client.AsyncDownloadFile(url,path)
                    Trace.TraceInformation(sprintf "Image %O downloaded" path)
                with
                |   :? WebException as we ->
                    Trace.TraceError(sprintf "Got WebException(%s) when downloading %O to %O" (we.ToString()) url path)
                semaphore.Release() |> ignore
                return ()
            }

        let mbProcessor = MailboxProcessor<ImageDownloaderMsg>.Start(fun inbox ->
            let rec messageLoop state = async {
                let! msg = inbox.Receive()
                match msg with
                |   DownloadImage(url, localFilename) ->
                    //match state.ShutdownChannel with
                    //|   Some(_) ->
                    //    Trace.TraceInformation(sprintf "ignoring image downloading %O as shutdown has started" url)
                    //    return! messageLoop state
                    //|   None ->
                    if not (File.Exists(localFilename)) then
                        async {
                            do! download url localFilename 
                            inbox.Post(DownloadedImage(url, localFilename))
                        } |> Async.Start
                        return! messageLoop {state with ActiveDownloads = state.ActiveDownloads + 1}
                    else
                        Trace.TraceInformation(sprintf "Image %s is already on disk" localFilename)
                        return! messageLoop state
                |   DownloadedImage(_,_) ->
                    match state.ShutdownChannel with
                    |   Some(sc) ->
                        if state.ActiveDownloads = 1 then
                            Trace.TraceInformation("Gracefully finished Image downloader")
                            sc.Reply()
                        else
                            Trace.TraceInformation(sprintf "Gracefully shutting down image downloader (%d active downloads still active)" (state.ActiveDownloads-1))
                    |   None -> ()
                    return! messageLoop {state with ActiveDownloads = state.ActiveDownloads - 1}
                |   ShutdownImageDownloader(channel) ->
                    if state.ActiveDownloads = 0 then
                        Trace.TraceInformation("Image downloader has shutted down")
                        channel.Reply()
                    else
                        Trace.TraceInformation(sprintf "Graceful shut down image downloader started (%d active downloads still active)" state.ActiveDownloads)
                    return! messageLoop {state with ShutdownChannel = Some(channel)}

                }
            let initialState = {
                ActiveDownloads = 0
                ShutdownChannel = None
            }
                
            messageLoop initialState)

        member _.Post(url,path) =
            mbProcessor.Post(DownloadImage(url,path))

        member _.Shutdown() =
            mbProcessor.PostAndAsyncReply(fun r -> ShutdownImageDownloader(r))

    type PetCardDownloaderCommand =
    |   ProcessArtId of artID:string
    |   ProcesssingFinished of artID:string
    |   ImageDownloaderShuttedDown
    |   ShutdownPetCardDownloader of AsyncReplyChannel<unit>

    type PetCardDownloaderState = {
        activeJobs: int
        finishedEvent : AsyncReplyChannel<unit> option
        imageDownloaderRunning : bool
    }

    type MissingArtSetProcessorCommand =
    |   InitialLoad of filename:string
    |   AddNewEntry of artID:string
    |   CheckIfInSet of artID:string * AsyncReplyChannel<bool>

    type PetCardDownloader(maxConcurrentCardDownloads, maxConcurrentImageDownloads, dbPath:string) =
        let semaphore = new SemaphoreSlim(maxConcurrentCardDownloads)
        
        let missingArtSetPath = Path.Combine(dbPath,"missingArtSet.json")

        let missingArtSetProcessor = MailboxProcessor<MissingArtSetProcessorCommand>.Start(fun inbox ->
            let rec loop oldState = async {
                let missingSet,persistenceFile = oldState
                match! inbox.Receive() with
                |   InitialLoad filename ->
                    let! loadedEntries = async {
                        if File.Exists filename then
                            let! loaded = File.ReadAllLinesAsync filename |> Async.AwaitTask
                            Trace.TraceInformation(sprintf "Loaded %d missing entry records" loaded.Length)
                            return Seq.ofArray loaded
                        else
                            Trace.TraceInformation("Nothing to load (missing entry records)")
                            return Seq.empty
                        }
                    return! loop (Set.ofSeq loadedEntries, filename)
                |   AddNewEntry artId ->
                    let extendedSet = Set.add artId missingSet
                    do! File.AppendAllLinesAsync(persistenceFile, (Seq.singleton artId)) |> Async.AwaitTask
                    // do! File.WriteAllLinesAsync(persistenceFile, Set.toArray extendedSet) |> Async.AwaitTask
                    return! loop (extendedSet, persistenceFile)
                |   CheckIfInSet(artId, replyChannel) ->
                    replyChannel.Reply(Set.contains artId missingSet)
                    return! loop (missingSet, persistenceFile)
                }
            loop (Set.empty,""))
        let imageDownloadProcessor = ImageDownloader(maxConcurrentImageDownloads)
        do
            if not(Directory.Exists(dbPath)) then
                Directory.CreateDirectory(dbPath) |> ignore
            missingArtSetProcessor.Post(InitialLoad missingArtSetPath)

        let extractImageJobs (card:CardJson.Root) =
            let artId = card.Pet.Art
            let artDirPath = Path.Combine(dbPath, artId)
            if not (Directory.Exists(artDirPath)) then
                Directory.CreateDirectory(artDirPath) |> ignore
            let photoThumbOutPaths = card.Pet.Photos |> Seq.map (fun photo -> Path.Combine(artDirPath, sprintf "%d.jpg" photo.Id))
            let photoThumbUrls = card.Pet.Photos |> Seq.map (fun photo -> new System.Uri(sprintf "%s/images%s" urlPrefix photo.Thumb)) 
            let imageDownloadCommands = Seq.zip photoThumbUrls photoThumbOutPaths
            imageDownloadCommands

        let checkArtId postImageDownloadRequest artId = async {
                //Trace.TraceInformation(sprintf "Processing %s" artId)
                let! previouslyMissing = missingArtSetProcessor.PostAndAsyncReply(fun r -> CheckIfInSet(artId,r))
                if not previouslyMissing then
                    let artDirPath = Path.Combine(dbPath,artId)
                    let cardPath = Path.Combine(artDirPath, "card.json")
                    let! imageJobs = async {
                        let! localCardCheckResult = async {
                            if File.Exists cardPath then
                                let! cardText = File.ReadAllTextAsync(cardPath) |> Async.AwaitTask
                                try
                                    return Some(CardJson.Parse(cardText))
                                with
                                |   _ -> return None
                            else
                                return None
                            }
                        match localCardCheckResult with
                        |   Some(card) ->
                            Trace.TraceInformation(sprintf "Valid card for %s already exists locally" artId)
                            return extractImageJobs card
                        |   None ->
                            // local snapshot not found, checking remote
                            do! semaphore.WaitAsync() |> Async.AwaitTask
                            Trace.TraceInformation(sprintf "Checking remote card for %s" artId)
                            let! cardExtractResult = tryExtractCard artId
                            semaphore.Release() |> ignore
                            // Trace.TraceInformation(sprintf "remote card for %s checked" artId)

                            match cardExtractResult with
                            |   None ->
                                // Trace.TraceInformation(sprintf "Card for %s does not exist remotely" artId)
                                missingArtSetProcessor.Post(AddNewEntry artId)
                                return Seq.empty
                            |   Some(card) ->
                                Trace.TraceInformation(sprintf "Valid card acquired for %s" artId)
                                Directory.CreateDirectory(artDirPath) |> ignore
                                File.WriteAllTextAsync(cardPath,card.JsonValue.ToString(JsonSaveOptions.None)) |> ignore
                                return extractImageJobs card
                        }
                            
                    Seq.iter postImageDownloadRequest imageJobs
                
            }

        let mbProcessor = MailboxProcessor.Start(fun inbox ->
            let rec loop (state:PetCardDownloaderState) =  async {
                match! inbox.Receive() with
                |   ProcessArtId artId ->
                    //Trace.TraceInformation(sprintf "Starting job for artId %s" artId)
                    match state.finishedEvent with
                    |   Some _ ->
                        Trace.TraceWarning(sprintf "will not process artId %s as shutdown has started" artId)
                        ()
                    |   None ->
                        let processJob = async {
                                do! checkArtId (fun job -> imageDownloadProcessor.Post(job)) artId
                                inbox.Post(ProcesssingFinished artId)
                            }
                        processJob |> Async.Start
                    return! loop {state with activeJobs = state.activeJobs + 1}
                |   ProcesssingFinished artId ->
                    // Trace.TraceInformation(sprintf "Finished processing %s" artId)
                    if state.activeJobs = 1 then
                        match state.finishedEvent with
                        |   Some _ ->
                            Trace.TraceInformation("Card downloader. All cards processed. Waitng image downloader to shutdown")
                            let imageDownloaderShutdownTask = async {
                                do! imageDownloadProcessor.Shutdown()
                                inbox.Post(ImageDownloaderShuttedDown)
                                }
                            imageDownloaderShutdownTask |> Async.Start
                        |   None -> ()
                    return! loop {state with activeJobs = state.activeJobs - 1}
                |   ShutdownPetCardDownloader exitChannel ->
                    Trace.TraceInformation(sprintf "Shutting down card downloader: %d to download" state.activeJobs)
                    return! loop {state with finishedEvent = Some(exitChannel)}
                |   ImageDownloaderShuttedDown ->
                    match state.finishedEvent with
                    |   Some(channel) ->
                        if state.activeJobs = 0 then
                            Trace.TraceInformation("Image downloader & Card downloader has shutdown")
                            channel.Reply()
                    |   None -> ()
                    return! loop state
                }
            let firstState = {
                activeJobs = 0
                finishedEvent = None
                imageDownloaderRunning = true
            }
            loop firstState)

        member _.Post msg =
            mbProcessor.Post(msg)

        member _.PostWithReply reply =
            mbProcessor.PostAndAsyncReply(reply)

