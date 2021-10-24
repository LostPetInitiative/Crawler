module Crawlers.Pet911

open System
open System.IO
open Xunit

open Kashtanka
open Kashtanka.Common
open Kashtanka.Parsers
open Kashtanka.Crawler
open Kashtanka.SemanticTypes

open Kashtanka.CrawlerPet911
open System.Threading

let userAgent = "KashtankaTestRunner/0.0.1"

let sem = new SemaphoreSlim(1)
let mutable cache:Map<string,Result<Downloader.DownloadedFileWithMime,string>> = Map.empty
let cachedFetch (fetch: string -> Async<Result<Downloader.DownloadedFileWithMime,string>>) =
    fun url ->
        async {
                do! sem.WaitAsync() |> Async.AwaitTask
                match Map.tryFind url cache with
                |   Some res ->
                    sem.Release() |> ignore
                    System.Diagnostics.Trace.TraceInformation(sprintf "cache hit: %s" url)
                    return res
                |   None ->
                    let! fetched = fetch url
                    System.Diagnostics.Trace.TraceInformation(sprintf "downloaded: %s" url)
                    cache <- Map.add url fetched cache
                    System.Diagnostics.Trace.TraceInformation(sprintf "cache size: %d" cache.Count)
                    sem.Release() |> ignore
                    return fetched

        }

type Pet911RealCrawling() =
    let tempDir = Path.Combine(Path.GetTempPath(),Path.GetRandomFileName())

    let imageDownloadingAgent =
        let fetchUrl = Downloader.httpDownload userAgent 60000
        new Downloader.Agent(1, Downloader.defaultDownloaderSettings, fetchUrl)
    let downloadImage (desc:RemoteResourseDescriptor) = (cachedFetch imageDownloadingAgent.Download) desc.url
    
    do
        Directory.CreateDirectory(tempDir) |> ignore
        System.Diagnostics.Trace.WriteLine(sprintf "Temp dir is %s" tempDir) |> ignore
        
    interface IDisposable with
        member _.Dispose() =
            Directory.Delete(tempDir,true)
            

    [<Fact>]
    member _.``Acquiring photo file created`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rl476712/162560784360e4cea36deb30.11666472.jpeg"
                url= "https://pet911.ru/upload/Pet_thumb_162560784360e4cea36deb30.11666472.jpeg"
            }
            let agent =
                constructPet911ImageProcessor tempDir downloadImage (fun _ -> ())

            agent.Enqueue(descr);
            do! agent.Shutdown()

            Assert.True(File.Exists(Path.Combine(tempDir,"rl476712","162560784360e4cea36deb30.11666472.jpeg")))
        }

    [<Fact>]
    member _.``Photo mime is written`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rl476712/162560784360e4cea36deb30.11666472.jpeg"
                url= "https://pet911.ru/upload/Pet_thumb_162560784360e4cea36deb30.11666472.jpeg"
            }
            let agent =
                constructPet911ImageProcessor tempDir downloadImage (fun _ -> ())

            agent.Enqueue(descr);
            do! agent.Shutdown()

            let! mime = File.ReadAllTextAsync(Path.Combine(tempDir,"rl476712","162560784360e4cea36deb30.11666472.jpeg.mime")) |> Async.AwaitTask
            Assert.Equal("image/jpeg",mime)
        }

    [<Fact>]
    member _.``Photo is checked`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rl476712/162560784360e4cea36deb30.11666472.jpeg"
                url= "https://pet911.ru/upload/Pet_thumb_162560784360e4cea36deb30.11666472.jpeg"
            }

            let mutable check = false

            let agent =
                constructPet911ImageProcessor tempDir downloadImage (fun (_,res) -> check <- not(hasFailed res))

            agent.Enqueue(descr);
            do! agent.Shutdown()

            Assert.True(check)
        }