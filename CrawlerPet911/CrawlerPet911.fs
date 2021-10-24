module Kashtanka.CrawlerPet911

open Crawler
open FileCollector
open System.IO

let moduleName = "Pet911ImageProcessor"
let traceError msg = Tracing.traceError moduleName msg
let traceWarning msg = Tracing.traceWarning moduleName msg
let traceInfo msg = Tracing.traceInfo moduleName msg

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
            FileCollector.saveLocalFile file (Path.Combine(baseDir, cardId, photoId))
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