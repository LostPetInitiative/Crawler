module Kashtanka.FileCollector

open System.IO

let tryLoadLocalFile path =
    async {
        try
            if File.Exists path then
                let! content = Async.AwaitTask <| File.ReadAllBytesAsync(path)
                let! mime = 
                    async {
                        let mimeFilePath = sprintf "%s.mime" path
                        if File.Exists mimeFilePath then
                            let! mimeStr = Async.AwaitTask <| File.ReadAllTextAsync(mimeFilePath)
                            return Some mimeStr
                        else
                            return None
                    }
                    
                return Ok(Some(Downloader.Binary(content),mime))
            else
                return Ok(None)
        with
        |   exc -> return Error(exc.ToString())
    }

let saveLocalFile (file:Downloader.DownloadedFileWithMime) path = 
    async {
        try
            let fileData,mime = file
            let writeFileTask =
                match fileData with
                |   Downloader.Binary bytes -> File.WriteAllBytesAsync(path,bytes)
                |   Downloader.Text str -> File.WriteAllTextAsync(path,str)
            do! Async.AwaitTask writeFileTask
            match mime with
            |   None -> ()
            |   Some m ->
                let mimePath = sprintf "%s.mime" path
                do! File.AppendAllTextAsync(mimePath,m) |> Async.AwaitTask
            return Ok()
        with
        |   exc -> return Error(exc.ToString())
    }