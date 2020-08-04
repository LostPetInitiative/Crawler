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
        Trace.TraceInformation(sprintf "Requesting %s (%s)" fullURL jsonBody)
        Http.AsyncRequest(
            fullURL,
            headers = [
                        HttpRequestHeaders.ContentType HttpContentTypes.Json;
                        //HttpRequestHeaders.Accept HttpContentTypes.Json;
                        //HttpRequestHeaders.UserAgent agentName;
                        //HttpRequestHeaders.Origin urlPrefix
                        ],
            httpMethod = "POST",
            body = TextRequest jsonBody, silentHttpErrors = false, timeout = 10)

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
            // do! Async.SwitchToThreadPool()
            let! response = apiRequest "/api/view-pet" jsonBody
            Trace.TraceInformation(sprintf "Got reply for %s" cardId)
            match response.StatusCode with
            |   HttpStatusCodes.OK ->
                match response.Body with
                |   HttpResponseBody.Text responseStr ->
                    let responseJson = CardJson.Parse(responseStr)
                    return Some(responseJson)
                |   HttpResponseBody.Binary _ ->
                    Trace.TraceError(sprintf "Binary body returned when extracting a pet card \'%s\'" cardId)
                    return None
            |   HttpStatusCodes.NotFound ->
                    return None
            |   _ ->
                Trace.TraceError(sprintf "Non successful error code (%d) when extracting the pet card \'%s\'" response.StatusCode cardId)
                return None
        }
    
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

        let mbProcessor = MailboxProcessor<System.Uri*string>.Start(fun inbox ->
            let rec messageLoop() = async {
                let! msg = inbox.Receive()
                let url, localFilename = msg
                if not (File.Exists(localFilename)) then
                    download url localFilename |> Async.Start
                return! messageLoop()
                }
            messageLoop())

        member _.Post(url,path) =
            mbProcessor.Post(url,path)

    type PetCardDownloaderCommand =
    |   ProcessArtId of artID:string
    |   ExitAfter of numJobs:int
    |   Start of AsyncReplyChannel<unit>

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
                    do! File.WriteAllLinesAsync(persistenceFile, Set.toArray extendedSet) |> Async.AwaitTask
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
                Trace.TraceInformation(sprintf "Processing %s" artId)
                let! previouslyMissing = missingArtSetProcessor.PostAndAsyncReply(fun r -> CheckIfInSet(artId,r))
                if not previouslyMissing then
                    let artDirPath = Path.Combine(dbPath,artId)
                    let cardPath = Path.Combine(artDirPath, "card.json")
                    let! imageJobs = async {
                        if File.Exists cardPath then
                            let! existingRecordText = File.ReadAllTextAsync(cardPath) |> Async.AwaitTask
                            let card = CardJson.Parse existingRecordText
                            return extractImageJobs card
                        else
                            // local snapshot not found, checking remote
                            do! semaphore.WaitAsync() |> Async.AwaitTask
                            let! cardExtractResult = tryExtractCard artId
                            semaphore.Release() |> ignore
                            Trace.TraceInformation(sprintf "Downloaded card for %s" artId)

                            match cardExtractResult with
                            |   None ->
                                missingArtSetProcessor.Post(AddNewEntry artId)
                                return Seq.empty
                            |   Some(card) ->
                                File.WriteAllTextAsync(cardPath,card.JsonValue.ToString(JsonSaveOptions.None)) |> ignore
                                return extractImageJobs card
                        }
                            
                    Seq.iter postImageDownloadRequest imageJobs
            }

        let mbProcessor = MailboxProcessor.Start(fun inbox ->
            let rec loop state =  async {
                let jobsLeft, (finishedChannel:AsyncReplyChannel<unit> option) = state
                if jobsLeft = 0 then
                    finishedChannel.Value.Reply()
                else
                    match! inbox.Receive() with
                    |   ProcessArtId artId ->
                        do! checkArtId (fun job -> imageDownloadProcessor.Post(job)) artId
                        return! loop ((jobsLeft-1), finishedChannel)
                    |   ExitAfter numJobs ->
                        return! loop (numJobs, finishedChannel)
                    |   Start exitChannel ->
                        return! loop (jobsLeft, Some(exitChannel))
                }
            loop (System.Int32.MaxValue, None))

        member _.Post msg =
            mbProcessor.Post(msg)

        member _.PostWithReply reply =
            mbProcessor.PostAndAsyncReply(reply)

