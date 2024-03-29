﻿namespace CrawlerLib

open Newtonsoft.Json
open FSharp.Data
open System.Diagnostics
open System.Net
open System.IO
open System.Threading
open SixLabors.ImageSharp
open Google.Apis
open Google.Apis.Drive.v3

module Pet911ru =
    type Animal = 
    |   dog = 1
    |   cat = 2
    type Sex = 
    |   unknown = 1
    |   male = 2
    |   female = 3
    type CardType =
    |   lost = 1
    |   found = 2
    type PetPhoto = {
        /// Full photo Filename with extension
        Filename: string
        /// Thumbnail photo filename with extension
        ThumbFilename: string
    }
    type Author = {
        username: string
        phone: string
        email: string
    }
    type PetCard = {
        art: string
        photos : PetPhoto[]
        animal: Animal
        sex: Sex
        address: string
        latitude: string
        longitude: string
        date: string // %Y-%m-%dT%H:%M:%SZ
        ``type``: CardType
        description: string
        author: Author
    }

    type CardSearchResult =
        {
            art: string
            url: string
        }

    let parseCard cardText : PetCard =
        // TODO: do html parsing here
        failwith "not implemented"

    let urlPrefix = @"https://pet911.ru"
    // let agentName = "LostPetInitiative:Crawler-pet911.ru / 0.1 (https://github.com/LostPetInitiative/Crawler-pet911.ru)"
    let agentName = "LostPetInitiative:Kashtanka-crawler / 0.1 (https://kashtanka.pet/)"

    let private apiRequest path =
        let fullURL = sprintf "%s%s" urlPrefix path
        // Trace.TraceInformation(sprintf "Requesting %s (%s)" fullURL jsonBody)
        Http.AsyncRequest(
            fullURL,
            headers = [
                        HttpRequestHeaders.ContentType HttpContentTypes.Html;
                        //HttpRequestHeaders.Accept HttpContentTypes.Json;
                        HttpRequestHeaders.UserAgent agentName;
                        //HttpRequestHeaders.Origin urlPrefix
                        ],
            httpMethod = "GET",
            silentHttpErrors = true, timeout = 60000)

    /// Extracts the URL for some cardID (if any)
    let tryFetchCardPageURL cardId = 
        async {
            let path = sprintf "/Челябинск/найдена/собака/%s" cardId // the city, type and animal can be arbitrary and does not affect the results
            let! response = apiRequest path
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
    let tryExtractRemoteCard cardId =
        let path = sprintf "/Челябинск/найдена/собака/%s" cardId // the city, type and animal can be arbitrary and does not affect the results
        async {
            let! responseOption =
                async {
                    try
                        let! response = apiRequest path
                        // Trace.TraceInformation(sprintf "Got reply for %s" cardId)
                        return Ok response
                    with
                    |   :? WebException as we ->
                        if we.Status = WebExceptionStatus.ProtocolError then
                            //Trace.TraceInformation(sprintf "WE %O for %s" we  cardId);
                            ()
                        else
                            Trace.TraceInformation(sprintf "WE %O for %s status %O" we  cardId we.Status);
                        return Error(we.ToString())
                }
            match responseOption with
            |   Ok response ->
                match response.StatusCode with
                |   HttpStatusCodes.OK ->
                    let contentTypeFound, contentType = response.Headers.TryGetValue HttpResponseHeaders.ContentType
                    if contentTypeFound then
                        if contentType.Contains(@"text/html") then
                            match response.Body with
                            |   HttpResponseBody.Text responseStr ->
                                let card = parseCard responseStr
                                return Ok(Some(card))
                            |   HttpResponseBody.Binary _ ->
                                let errMsg = sprintf "Binary body returned when extracting a pet card \'%s\'" cardId
                                //Trace.TraceError(errMsg)
                                return Error(errMsg)
                        else
                            let errMsg = sprintf "Content type \'%s\' is not JSON for \'%s\'" contentType cardId
                            //Trace.TraceError(errMsg)
                            return Error(errMsg)
                    else
                        let errMsg = sprintf "The response for artId %s does not contain ContentType header" cardId
                        //Trace.TraceError(errMsg)
                        return Error(errMsg)
                |   HttpStatusCodes.NotFound ->
                        Trace.TraceInformation(sprintf "Got 404 for %s" cardId)
                        return Ok(None)
                |   _ ->
                    let errMsg = sprintf "Non successful error code (%d) when extracting the pet card \'%s\'" response.StatusCode cardId
                    //Trace.TraceError(errMsg)
                    return Error(errMsg)
            |   Error msg -> return Error msg
        }

    type ImageDownloaderState = {
        ActiveDownloads: int
        ShutdownChannel: AsyncReplyChannel<unit> option
    }

    type ProxySupportedHttpClientFactory()=
        inherit Google.Apis.Http.HttpClientFactory()

        override s.CreateHandler(args) =
            let proxy = new WebProxy("http://10.0.10.24:3128", true, null, null)

            let handler = new System.Net.Http.HttpClientHandler()
            handler.Proxy <- proxy
            handler.UseProxy <- true

            upcast handler


    type ImageDownloaderMsg =
    |   DownloadImage of photo:PetPhoto * artId:string
    |   DownloadedImage of photo:PetPhoto
    |   ShutdownImageDownloader of AsyncReplyChannel<unit>

    type ImageDownloader(dbPath, maxConcurrentDownloads911, maxConcurrentDownloadsGoogle, googleApiKey) =
        let concurrentTasksSemaphore = new SemaphoreSlim(System.Environment.ProcessorCount * 2)
        let random = System.Random()
        let minIntervalBetweenGoogleAPIRequests = System.TimeSpan.FromSeconds(10.0)
        let latestGoogleRequestLock = obj()
        let mutable latestGoogleRequest = System.DateTime.Now
        let semaphore911 = new SemaphoreSlim(maxConcurrentDownloads911)
        let semaphoreGoogle = new SemaphoreSlim(maxConcurrentDownloadsGoogle)
        let googleDriveServiceInitializer = Google.Apis.Services.BaseClientService.Initializer()
        let mutable googleDriveService:DriveService = null
        
        let validateLocalImage imagePath = async {
            if File.Exists(imagePath) then
                try
                    use! image = (Image.LoadAsync(imagePath, cancellationToken= CancellationToken()) |> Async.AwaitTask)
                    // Trace.TraceInformation(sprintf "valid local image %s found" imagePath)
                    return true
                with
                |   _ ->
                    File.Delete(imagePath)
                    // Trace.TraceWarning(sprintf "Invalid local image %s. Marked for re-download" imagePath)
                    return false
            else
                return false
            }

        let mimeToFormat (mimeStr:string) = 
            if mimeStr.Contains("image/jpeg") || mimeStr.Contains("image/jpg") then
                Some "jpg"
            elif mimeStr.Contains("image/png") then
                Some "png"
            else
                None

        let downloadHttpHostedPhotoAsBrowser (url:string) = 
            async {
                if System.String.IsNullOrEmpty(url) then
                    return Error("empty url")
                else
                    do! semaphoreGoogle.WaitAsync() |> Async.AwaitTask
                    let res = async {
                        try
                            try
                                let headers = [
                                    HttpRequestHeaders.UserAgent @"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:79.0) Gecko/20100101 Firefox/79.0";
                                    "DNT","1";
                                    "Accept",@"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8";
                                    "Accept-Language",@"en-GB,en;q=0.5"
                                    "Upgrade-Insecure-Requests","1";
                                    "Pragma",@"no-cache";
                                    "Cache-Control", @"no-cache"
                                    ]
                                let! response = Http.AsyncRequest(url,
                                                                    headers = headers,
                                                                    httpMethod = "GET", silentHttpErrors = true,
                                                                    timeout = 30000)
                                match response.StatusCode with
                                |   HttpStatusCodes.OK ->
                                    match Map.tryFind HttpResponseHeaders.ContentType response.Headers with
                                    |   Some contentType ->
                                        let format = mimeToFormat contentType
                                        let content =
                                            match response.Body with
                                            |   HttpResponseBody.Text _ -> None
                                            |   HttpResponseBody.Binary bin ->
                                                Some(bin)
                                        match format,content with
                                        |   Some(imageFormat),Some(data)->
                                            return Ok(imageFormat,data)
                                        |   _, _ ->
                                            let errMsg = "invalid content or response body type"
                                            Trace.TraceError(errMsg)
                                            return Error errMsg
                                    |   None ->
                                        let errMsg = "Got HTTP response with no contentType header for image"
                                        Trace.TraceError(errMsg)
                                        return Error errMsg
                                |   _ ->
                                    let errMsg = sprintf "Got not successful HTTP code: %d. %O" response.StatusCode response.Body
                                    Trace.TraceError(errMsg)
                                    return Error errMsg
                            finally
                                semaphoreGoogle.Release() |> ignore
                        with
                        |   we ->
                            let errMsg = we.ToString()
                            Trace.TraceError(errMsg)
                            return Error errMsg
                    }
                    return! res
            }

        let downloadHttpHostedPhoto (url:string) = 
            async {
                if System.String.IsNullOrEmpty(url) then
                    return Error("empty url")
                else
                    do! semaphore911.WaitAsync() |> Async.AwaitTask
                    let res = async {
                        try
                            try
                                let headers = [
                                    HttpRequestHeaders.UserAgent agentName;
                                    ]
                                let! response = Http.AsyncRequest(url,
                                                                    headers = headers,
                                                                    httpMethod = "GET", silentHttpErrors = true,
                                                                    timeout = 30000)
                                match response.StatusCode with
                                |   HttpStatusCodes.OK ->
                                    match Map.tryFind HttpResponseHeaders.ContentType response.Headers with
                                    |   Some contentType ->
                                        let format = mimeToFormat contentType
                                        let content =
                                            match response.Body with
                                            |   HttpResponseBody.Text _ -> None
                                            |   HttpResponseBody.Binary bin ->
                                                Some(bin)
                                        match format,content with
                                        |   Some(imageFormat),Some(data)->
                                            return Ok(imageFormat,data)
                                        |   _, _ ->
                                            let errMsg = "invalid content or response body type"
                                            Trace.TraceError(errMsg)
                                            return Error errMsg
                                    |   None ->
                                        let errMsg = "Got HTTP response with no contentType header for image"
                                        Trace.TraceError(errMsg)
                                        return Error errMsg
                                |   _ ->
                                    let errMsg = sprintf "Got not successful HTTP code: %d. %O" response.StatusCode response.Body
                                    Trace.TraceError(errMsg)
                                    return Error errMsg
                            finally
                                semaphore911.Release() |> ignore
                        with
                        |   we ->
                            let errMsg = we.ToString()
                            Trace.TraceError(errMsg)
                            return Error errMsg
                    }
                    return! res
            }

        let downloadPhoto (photo:PetPhoto) petDir = async {
            let downloadPet911Thumb() = async {
                if photo.ThumbFilename.Length > 0 then
                    return! downloadHttpHostedPhoto (sprintf "%s/upload/%s" urlPrefix photo.ThumbFilename)
                else
                    let errorMsg = "thumb URL is empty"
                    Trace.TraceError(errorMsg)
                    return Error(errorMsg)
            }
                
            //let downloadGoogleThumb() = async {
            //    if photo.Thumb.Length > 0 then
            //        return! downloadHttpHostedPhotoAsBrowser photo.ThumbGoogle
            //    else
            //        let errorMsg = "google thumb URL is empty"
            //        Trace.TraceError(errorMsg)
            //        return Error(errorMsg)
            //}

            let saveSuccessfullDownload arg = async {
                let format,content = arg
                let outFilename = Path.Combine(petDir, photo.ThumbFilename)
                do! File.WriteAllBytesAsync(outFilename, content) |> Async.AwaitTask
                // Trace.TraceInformation(sprintf "Thumb image %O downloaded (%s) to %s" photo.Id format outFilename)
                return outFilename
                }

            let! thumbDownloaded = downloadPet911Thumb() 
            match thumbDownloaded with
            |   Ok(format,content) ->
                Trace.TraceInformation(sprintf "got valid thumb from pet911.ru")
                //Trace.TraceInformation(sprintf "got valid thumb from google for pet %d" photo.PetId)
                let! outFilename = saveSuccessfullDownload(format,content)
                return Ok(outFilename)
            |   Error(thumbError) ->
                //let! thumb2Downloaded = downloadPet911Thumb()
                //match thumb2Downloaded with
                //|   Ok(format,content) ->
                //    Trace.TraceInformation(sprintf "got valid thumb from pet911.ru for pet %d" photo.PetId)
                //    let! outFilename = saveSuccessfullDownload(format,content)
                //    return Ok(outFilename)
                //|   Error(thumb2error) ->
                //    let errorMsg = sprintf "Failed to get either of thumbs: (%s) and (%s)" thumbError thumb2error
                //    Trace.TraceError(errorMsg)
                //    return Error(errorMsg)
                return Error(thumbError)
        }
        
        let mbProcessor = MailboxProcessor<ImageDownloaderMsg>.Start(fun inbox ->
            let rec messageLoop state = async {
                let! msg = inbox.Receive()
                match msg with
                |   DownloadImage(photo,artId) ->
                    async {
                        do! concurrentTasksSemaphore.WaitAsync() |> Async.AwaitTask
                        try
                            let petDir = Path.Combine(dbPath,artId)
                            let petImage = Path.Combine(petDir, photo.ThumbFilename)
                            let! localValid = validateLocalImage petImage
                            if not localValid then
                                Trace.TraceInformation(sprintf "Queuing to download image %s (for %s)" photo.ThumbFilename artId)
                                let! photoDownloadResult =  downloadPhoto photo petDir 
                                Trace.TraceInformation(sprintf "Processed download image %s job (for %s)" photo.ThumbFilename artId)
                                ()
                            else
                                Trace.TraceInformation(sprintf "Valid image %s (for %s) is already on disk" photo.ThumbFilename artId)
                            inbox.Post(DownloadedImage(photo))
                            return ()
                        finally
                            concurrentTasksSemaphore.Release() |> ignore
                    } |> Async.Start
                    
                    return! messageLoop {state with ActiveDownloads = state.ActiveDownloads + 1}
                |   DownloadedImage(_) ->
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
                        Trace.TraceInformation("Image downloader has shut down")
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

        do
            match googleApiKey with
            |   Some key ->
                Trace.TraceInformation ("Google API key is set")
                googleDriveServiceInitializer.ApiKey <- key
                googleDriveServiceInitializer.ApplicationName <- "pet911 crawler"
                // googleDriveServiceInitializer.HttpClientFactory <- ProxySupportedHttpClientFactory()
            |   None -> ()

            googleDriveService <- new DriveService(googleDriveServiceInitializer)
            

        member _.Post(msg) =
            mbProcessor.Post(msg)

        member _.Shutdown() =
            mbProcessor.PostAndAsyncReply(fun r -> ShutdownImageDownloader(r))

    type PetCardDownloaderCommand =
    |   ProcessArtId of artID:string
    |   ProcesssingFinished of artID:string
    |   ImageDownloaderShuttedDown
    |   ShutdownPetCardDownloader of AsyncReplyChannel<unit>

    type PetCardDownloaderState = {
        activeJobs: int
        downladedCardIds : string list
        finishedEvent : AsyncReplyChannel<unit> option
        imageDownloaderRunning : bool
    }

    type MissingArtSetProcessorCommand =
    |   InitialLoad of filename:string
    |   AddNewEntry of artID:string
    |   CheckIfInSet of artID:string * AsyncReplyChannel<bool>

    type PetCardDownloader(maxConcurrentCardDownloads, maxConcurrent911ImageDownloads, maxConcurrentGoogleImageDownloads, dbPath:string, googleApiKey) =
        let concurrentRequestSemaphore = new SemaphoreSlim(maxConcurrentCardDownloads)
        let concurrentTasksSemaphore = new SemaphoreSlim(System.Environment.ProcessorCount * 2)
        
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
        let imageDownloadProcessor = ImageDownloader(dbPath, maxConcurrent911ImageDownloads, maxConcurrentGoogleImageDownloads, googleApiKey)
        do
            if not(Directory.Exists(dbPath)) then
                Directory.CreateDirectory(dbPath) |> ignore
            missingArtSetProcessor.Post(InitialLoad missingArtSetPath)

        let extractImageJobs (card:PetCard) =
            let artId = card.art
            let artDirPath = Path.Combine(dbPath, artId)
            if not (Directory.Exists(artDirPath)) then
                Directory.CreateDirectory(artDirPath) |> ignore
            card.photos |> Seq.map (fun p -> DownloadImage(p,artId))

        let checkArtId postImageDownloadRequest (artId:string) = async {
                //Trace.TraceInformation(sprintf "Processing %s" artId)
                let tryParseLocalCard cardPath = async {
                    if File.Exists cardPath then
                        let! cardText = File.ReadAllTextAsync(cardPath) |> Async.AwaitTask
                        try
                            return Some(parseCard(cardText))
                        with
                        |   _ -> return None
                    else
                        return None
                    }

                let altArtId =
                    let altType = if artId.Substring(0,2) = "rl" then "rf" else "rl"
                    sprintf "%s%s" altType (artId.Substring(2))
                let altArtDirPath = Path.Combine(dbPath,altArtId)
                let altCardPath = Path.Combine(altArtDirPath, "card.json")
                let! altCardParseResult = tryParseLocalCard altCardPath
                match altCardParseResult with
                |   Some(_) ->
                    Trace.TraceInformation(sprintf "Valid ALT card for %s already exists locally" artId)
                    return () // if alternative card id exists, we do not check the requested card, as they are mutually exclusive
                |   None ->
                    let! previouslyMissing = missingArtSetProcessor.PostAndAsyncReply(fun r -> CheckIfInSet(artId,r))
                    if not previouslyMissing then
                        let artDirPath = Path.Combine(dbPath,artId)
                        let cardPath = Path.Combine(artDirPath, "card.json")
                        let! imageJobs = async {
                            let! localCardCheckResult = tryParseLocalCard cardPath
                            match localCardCheckResult with
                            |   Some(card) ->
                                Trace.TraceInformation(sprintf "Valid card for %s already exists locally" artId)
                                return extractImageJobs card
                            |   None ->
                                // local snapshot not found, checking remote
                                do! concurrentRequestSemaphore.WaitAsync() |> Async.AwaitTask
                                // Trace.TraceInformation(sprintf "Checking remote card for %s" artId)
                                let! cardExtractResult = tryExtractRemoteCard artId
                                concurrentRequestSemaphore.Release() |> ignore
                                // Trace.TraceInformation(sprintf "remote card for %s checked" artId)

                                match cardExtractResult with
                                |   Error msg ->
                                    Trace.TraceError(sprintf "Error obtaining remote card for %s: %s" artId msg)
                                    return Seq.empty
                                |   Ok result ->
                                    match result with
                                    |   None ->
                                        // Trace.TraceInformation(sprintf "Card for %s does not exist remotely" artId)
                                        missingArtSetProcessor.Post(AddNewEntry artId)
                                        return Seq.empty
                                    |   Some(card) ->
                                        Trace.TraceInformation(sprintf "Valid card acquired for %s" artId)
                                        Directory.CreateDirectory(artDirPath) |> ignore
                                        let serialized = JsonConvert.SerializeObject(card)
                                        File.WriteAllTextAsync(cardPath,serialized) |> ignore
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
                                do! concurrentTasksSemaphore.WaitAsync() |> Async.AwaitTask
                                try
                                    do! checkArtId (fun job -> imageDownloadProcessor.Post(job)) artId
                                    inbox.Post(ProcesssingFinished artId)
                                finally
                                    concurrentTasksSemaphore.Release() |> ignore
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
                    return! loop {state with activeJobs = state.activeJobs - 1; downladedCardIds = artId::state.downladedCardIds }
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
                downladedCardIds = []
                imageDownloaderRunning = true
            }
            loop firstState)

        member _.Post msg =
            mbProcessor.Post(msg)

        member _.PostWithReply reply =
            mbProcessor.PostAndAsyncReply(reply)

    let detectNewCardIds (latestDownloadedID:string) = [latestDownloadedID]
        

    (*
    let detectNewCardIds (latestDownloadedID:string) =
        // Trace.TraceInformation(sprintf "Checking newer than %s" latestDownloadedID)
        let latestNumID = System.Int32.Parse(latestDownloadedID.Substring(2))
        let pageSize = 100
        let rec getNewCardIDs pageNum gatheredNewIDs =
            let jsonReqestBody = sprintf """{
                                    "filters": {
                                      "pets": [],
                                      "type": [],
                                      "address": null,
                                      "latitude": null,
                                      "longitude": null,
                                      "date": null,
                                      "_date": null,
                                      "radius": null,
                                      "animal": [],
                                      "breed": null,
                                      "breeds": [],
                                      "color": [],
                                      "sex": [],
                                      "age": null,
                                      "collar": [],
                                      "sterile": [],
                                      "sortBy": "id",
                                      "searchByArt": null,
                                      "mapRadius": null
                                    },
                                    "pagination": {
                                      "pageSize": %d,
                                      "page": %d,
                                      "end": false
                                    }
                                }""" pageSize pageNum //"sortBy": "created_at",
            async {
                Trace.TraceInformation(sprintf "Getting new cards page %d (%d cards per page)" pageNum pageSize)
                try
                    let! response = apiRequest "/api/pets" jsonReqestBody
                    match response.StatusCode with
                    |   HttpStatusCodes.OK ->
                        match response.Body with
                        |   HttpResponseBody.Text responseStr->
                            let responseJson = CardsJson.Parse(responseStr)
                            let newerThenLatestDownloaded (p:CardsJson.Pet) = p.Id > latestNumID
                            let premiumOrDownloaded (p:CardsJson.Pet) = (p.PremiumStatus = 1) || (p.Id > latestNumID)

                            let latestIds =
                                responseJson.Pets
                                |> Seq.filter newerThenLatestDownloaded
                                |> Seq.map (fun card -> card.Art)
                                |> Set.ofSeq

                            let skippedFetchedCards =
                                responseJson.Pets
                                |> Seq.takeWhile premiumOrDownloaded
                                |> Seq.length

                            Trace.TraceInformation(sprintf "Got %d new cards in page %d" (Set.count latestIds) pageNum)
                            let accumulatedLatestIds = Set.union gatheredNewIDs latestIds
                            if skippedFetchedCards < pageSize then
                                Trace.TraceInformation("Found previously downloaded non premium IDs")
                                return Some(accumulatedLatestIds)
                            else
                                return! getNewCardIDs (pageNum+1) accumulatedLatestIds
                        |   HttpResponseBody.Binary _ ->
                            Trace.TraceError(sprintf "Binary body returned when searching for a new cards")
                            return None
                    |   _ ->
                        Trace.TraceError(sprintf "Non successful error code (%d) when searching for a new cards" response.StatusCode)
                        return None
                with
                |  :? WebException as ex1 ->
                    Trace.TraceError(sprintf "WebException during new cards fetch: %A" ex1)
                    return None
            }
        getNewCardIDs 0 Set.empty
*)