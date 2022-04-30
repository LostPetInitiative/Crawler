module Kashtanka.Images

open SixLabors.ImageSharp
open System.IO

let moduleName = "ImageProcessing"
let traceError msg = Tracing.traceError moduleName msg
let traceWarning msg = Tracing.traceWarning moduleName msg
let traceInfo msg = Tracing.traceInfo moduleName msg


let validateImage (image:Downloader.DownloadedFile) = async {
    match image with
    |   Downloader.Text _ -> return false
    |   Downloader.Binary bytes ->
        use memStream = new MemoryStream(bytes)
        try
            use! image = (Image.LoadAsync(memStream)) |> Async.AwaitTask
            return true
        with
        |   ex ->
            sprintf "Failed to validate image: %O" ex |> traceWarning
            return false
}

let mimeToExt (mimeStr:string) = 
    if mimeStr.Contains("image/jpeg") || mimeStr.Contains("image/jpg") then
        Some "jpg"
    elif mimeStr.Contains("image/png") then
        Some "png"
    elif mimeStr.Contains("image/webp") then
        Some "webp"
    else
        None

