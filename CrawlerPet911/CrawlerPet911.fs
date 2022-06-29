module Kashtanka.pet911.Crawler

open Kashtanka
open Downloader
open Crawler
open Utils
open SemanticTypes
open MissingResourceTracker
open FileCollector
open Parsers
open System.IO
open HtmlAgilityPack

let moduleName = "Pet911ImageProcessor"
let traceError msg = Tracing.traceError moduleName msg
let traceWarning msg = Tracing.traceWarning moduleName msg
let traceInfo msg = Tracing.traceInfo moduleName msg

let cardFilename = "card.html.dump"

let missingImagesFilename = "missingImages.txt"

let missingCardsFilename = "missingCards.txt"

let constructPet911ImageProcessor baseDir download =
    async {
        let! missingImagesTracker = createFileBackedMissingResourceTracker(Path.Combine(baseDir,missingImagesFilename))
        let tryGetLocal (descriptor:RemoteResourseDescriptor) =
            async {
                match parsePhotoId descriptor.ID with
                |   Some(cardId,photoId) ->
                    match! missingImagesTracker.Check descriptor.ID with
                    |   Error e -> return Error(sprintf "Error during local absence check: %s" e)
                    |   Ok knownToBeMissing ->
                        if knownToBeMissing then
                            return Ok(Some Absent)
                        else
                            match! tryLoadLocalFile(Path.Combine(baseDir, cardId, photoId)) with
                            |   Error e -> return Error(sprintf "Error during presence check: %s" e)
                            |   Ok localCheck ->
                                return localCheck |> Option.map (fun x -> Downloaded x) |> Ok
                |   None -> return Error(sprintf "Invalid photo id \"%s\"" descriptor.ID)
            }
        let saveLocal (descriptor:RemoteResourseDescriptor, lookupRes:RemoteResourseLookup) =            
            match parsePhotoId descriptor.ID with
            |   Some(cardId, photoId) ->
                match lookupRes with
                |   Absent ->
                    sprintf "Storing info that photo \"%s\" of card \"%s\" does not exist at remote side." photoId cardId |> traceInfo
                    missingImagesTracker.Add descriptor.ID
                |   Downloaded file ->
                    sprintf "Saving photo \"%s\" for card \"%s\" to disk." photoId cardId |> traceInfo
                    Directory.CreateDirectory(Path.Combine(baseDir, cardId)) |> ignore
                    saveLocalFile file (Path.Combine(baseDir, cardId, photoId))
            |   None ->
                async { return Error(sprintf "Invalid photo id \"%s\"" descriptor.ID) }
            
        let processResource (file:Downloader.DownloadedFileWithMime) =
            async {
                match! Images.validateImage (fst file) with
                |   true ->                    
                    return Ok()
                |   false -> return Error("Resource is not a valid image")
            }
        
        let callbacks: CrawlerCallbacks<unit> = {
            tryGetLocal = tryGetLocal
            saveLocal = saveLocal
            download = download
            processDownloaded = processResource
            }
        let agent = new Agent<unit>(callbacks)
        return agent
    }

let constructPet911CardProcessor baseDir download =
    async {
        let! missingCardsTracker = createFileBackedMissingResourceTracker(Path.Combine(baseDir,missingCardsFilename))
        let tryGetLocal (descriptor:RemoteResourseDescriptor) =
            async {
                match! missingCardsTracker.Check descriptor.ID  with
                |   Error er -> return Error er
                |   Ok missing ->
                        if missing then
                            return Absent |> Some |> Ok
                        else
                            let! localLoadRes = tryLoadLocalFile(Path.Combine(baseDir, descriptor.ID, cardFilename))
                            let resultMapper localCheck =
                                let optionMapper local = Downloaded local
                                localCheck |> Option.map optionMapper
                            return localLoadRes |> Result.map resultMapper
            }
        let saveLocal (descriptor:RemoteResourseDescriptor, data:RemoteResourseLookup) =
            match data with
            |   Downloaded file ->
                Directory.CreateDirectory(Path.Combine(baseDir, descriptor.ID)) |> ignore
                saveLocalFile file (Path.Combine(baseDir, descriptor.ID, cardFilename))
            |   Absent ->
                missingCardsTracker.Add descriptor.ID
                
        let processResource (file:Downloader.DownloadedFileWithMime) : Async<Result<PetCard,string>> =
            async {
                let fileData = fst file
                let text = Downloader.downloadedFileToText fileData
                let doc = new HtmlDocument()
                doc.LoadHtml(text)

                // TODO: use Result building monad here

                match getAnimalSpecies(doc) with
                |   Error er -> return Error(er)
                |   Ok(species) ->
                    match getAnimalSex(doc) with
                    |   Error er -> return Error(er)
                    |   Ok(sex) ->
                        match getAuthorName(doc) with
                        |   Error er -> return Error(er)
                        |   Ok(authorName) ->
                            match getAuthorMessage(doc) with
                            |   Error er -> return Error(er)
                            |   Ok(authorMessage) ->
                                match getCardId(doc) with
                                |   Error(er) -> return Error(er)
                                |   Ok(cardID) ->
                                    match getEventAddress(doc) with
                                    |   Error(er) -> return Error(er)
                                    |   Ok(eventAddress) ->
                                        match getEventTimeUTC(doc) with
                                        |   Error(er) -> return Error(er)
                                        |   Ok(eventTime) ->
                                            match getEventType(doc) with
                                            |   Error er -> return Error(er)
                                            |   Ok(eventType) ->
                                                match getPhotoUrls(doc) with
                                                |   Error er -> return Error(er)
                                                |   Ok(photoUrls) ->
                                                    match photoUrls |> Seq.map (fun x -> System.Uri(x) |> getPhotoId) |> Common.allResults with
                                                    |   Error er -> return Error(er)
                                                    |   Ok(barePhotoIds) ->
                                                        let photoIds = barePhotoIds |> List.map (fun x -> sprintf "%s/%s" cardID x)
                                                        match getEventCoords(text) with
                                                        | Error er -> return Error(er)
                                                        | Ok(lat,lon) ->
                                                            return Ok({
                                                                id=cardID;
                                                                photos= Array.ofSeq <| Seq.map2 (fun id url -> {ID=id;url=url}) photoIds photoUrls;
                                                                animal=species;
                                                                sex=sex;
                                                                address=eventAddress;
                                                                latitude=Some(lat);
                                                                longitude=Some(lon);
                                                                date=eventTime;
                                                                ``type``= eventType;
                                                                description=authorMessage;
                                                                author={
                                                                    name= authorName;
                                                                    phone = None;
                                                                    email = None;
                                                                }
                                                            })
            }
        
        let callbacks: CrawlerCallbacks<PetCard> = {
            tryGetLocal = tryGetLocal
            saveLocal = saveLocal
            download = download
            processDownloaded = processResource
            }
        let agent = new Agent<PetCard>(callbacks)
        return agent
    }

let constructCrawler baseDir download  = 
    async {        
        let! imageProcessor = constructPet911ImageProcessor baseDir download
        let! cardProcessor = constructPet911CardProcessor baseDir download

        let processImage descriptor = async { let! fullResult = imageProcessor.Process descriptor in return Result.map (fun _ -> ()) fullResult}

        let photosForCardCrawler = PhotosForCardCrawler.Agent(processImage)
        
        let processCard cardDescriptor =
            async {
                match! cardProcessor.Process cardDescriptor with
                |   Error er -> return Error (sprintf "Failed to process card %s: %s" cardDescriptor.ID er)
                |   Ok processedResource ->                    
                    match processedResource with
                    |   Missing -> return Ok()
                    |   Processed card ->
                        sprintf "Card %s parsed successfully" cardDescriptor.ID |> traceInfo
                        let! res = photosForCardCrawler.AwaitAllPhotos card.id card.photos                        
                        
                        // dumping json
                        let pipelineJsonPath = System.IO.Path.Combine(baseDir, cardDescriptor.ID, "card.json")
                        do! System.IO.File.WriteAllTextAsync(pipelineJsonPath, cardToPipelineJSON(card).ToString()) |> Async.AwaitTask

                        let wholeJsonPath = System.IO.Path.Combine(baseDir, cardDescriptor.ID, "cardFull.json")
                        do! System.IO.File.WriteAllTextAsync(wholeJsonPath, Newtonsoft.Json.Linq.JObject.FromObject(card).ToString()) |> Async.AwaitTask

                        sprintf "Card %s (and its %d photos) are processed" card.id card.photos.Length |> traceInfo
                        return res
            }

        return processCard
    }
    
