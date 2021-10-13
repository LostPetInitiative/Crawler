module Kashtanka.FileCollector

open System.IO

let obtainFile localPath url (fetch: string -> Async<Downloader.DownloadResult>) : Async<Downloader.DownloadResult> =
    async {
        if File.Exists(localPath) then
            let! content = Async.AwaitTask <| File.ReadAllBytesAsync(localPath)
            return Ok(Downloader.Binary(content),None) // todo: load proper mime type
        else
            let! downloadResult = fetch url
            match downloadResult with
            |   Error _ -> return downloadResult
            |   Ok downloaded ->
                match downloaded with // todo: write mime
                |   Downloader.Binary bytes, mime ->
                    do! File.WriteAllBytesAsync(localPath, bytes) |> Async.AwaitTask
                |   Downloader.Text text, mime ->
                    do! File.WriteAllTextAsync(localPath, text) |> Async.AwaitTask
                return downloadResult
    }