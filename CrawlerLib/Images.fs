module Kashtanka.Images

open SixLabors.ImageSharp
open System.IO

let validateImage (image:Downloader.DownloadedFile) = async {
    match image with
    |   Downloader.Text _ -> return false
    |   Downloader.Binary bytes ->
        use memStream = new MemoryStream(bytes)
        try
            use! image = (Image.LoadAsync(memStream)) |> Async.AwaitTask
            return true
        with
        |   _ -> return false
}

let mimeToExt (mimeStr:string) = 
    if mimeStr.Contains("image/jpeg") || mimeStr.Contains("image/jpg") then
        Some "jpg"
    elif mimeStr.Contains("image/png") then
        Some "png"
    else
        None

