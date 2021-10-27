module Kashtanka.CrawlerPet911

open Crawler
open SemanticTypes
open FileCollector
open Parsers.pet911
open System.IO
open HtmlAgilityPack

let moduleName = "Pet911ImageProcessor"
let traceError msg = Tracing.traceError moduleName msg
let traceWarning msg = Tracing.traceWarning moduleName msg
let traceInfo msg = Tracing.traceInfo moduleName msg

let cardFilename = "card.html.dump"

let constructPet911ImageProcessor baseDir download reportProcessed =
    let parseID (id:string) =
        // ID = cardID/photoID.ext
        let parts = id.Split([|'/'|])
        match parts with
        | [| cardId; photoId |] -> Some(cardId,photoId)
        |   _ -> None
    let tryGetLocal (descriptor:RemoteResourseDescriptor) =
        async {
            match parseID descriptor.ID with
            |   Some(cardId,photoId) ->
                return! tryLoadLocalFile(Path.Combine(baseDir, cardId, photoId))
            |   None -> return Error(sprintf "Invalid photo id \"%s\"" descriptor.ID)
        }
    let saveLocal (descriptor:RemoteResourseDescriptor, file:Downloader.DownloadedFileWithMime) =
        match parseID descriptor.ID with
        |   Some(cardId, photoId) ->
            Directory.CreateDirectory(Path.Combine(baseDir, cardId)) |> ignore
            saveLocalFile file (Path.Combine(baseDir, cardId, photoId))
        |   None ->
            async {return Error(sprintf "Invalid photo id \"%s\"" descriptor.ID)}
    let processResource (file:Downloader.DownloadedFileWithMime) =
        async {
            match! Images.validateImage (fst file) with
            |   true -> return Ok()
            |   false -> return Error("Resource is not a valid image")
        }
        
    let callbacks: CrawlerCallbacks<unit> = {
        tryGetLocal = tryGetLocal
        saveLocal = saveLocal
        download = download
        processResource = processResource
        reportProcessed = reportProcessed
        }
    let agent = new Agent<unit>(callbacks)
    agent

let constructPet911CardProcessor baseDir download reportProcessed =
    let tryGetLocal (descriptor:RemoteResourseDescriptor) =
        async {
            return! tryLoadLocalFile(Path.Combine(baseDir, descriptor.ID, cardFilename))
        }
    let saveLocal (descriptor:RemoteResourseDescriptor, file:Downloader.DownloadedFileWithMime) =
        Directory.CreateDirectory(Path.Combine(baseDir, descriptor.ID)) |> ignore
        saveLocalFile file (Path.Combine(baseDir, descriptor.ID, cardFilename))
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
                                                let photoUrlToID (url:string) =
                                                    if url.StartsWith(photoUrlPrefix) then
                                                        Ok(sprintf "%s/%s" cardID (url.ToLowerInvariant().Substring(photoUrlPrefix.Length)))
                                                    else Error(sprintf "Unexpected photo URL prefix in url %s" url)
                                                match photoUrls |> Seq.map photoUrlToID |> Common.allResults with
                                                |   Error er -> return Error(er)
                                                |   Ok(photoIds) ->
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
        processResource = processResource
        reportProcessed = reportProcessed
        }
    let agent = new Agent<PetCard>(callbacks)
    agent