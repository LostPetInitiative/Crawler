module Crawlers.Pet911

open System
open System.IO
open Xunit

open Kashtanka
open Kashtanka.Common
open Kashtanka.Crawler
open Kashtanka.SemanticTypes

open Kashtanka.pet911.Crawler
open System.Threading

let userAgent = "KashtankaTestRunner/1.0.0"

let traceInfo = Tracing.traceInfo "Tests"

let sem = new SemaphoreSlim(1)
let mutable cache:Map<string,Downloader.DownloadResult> = Map.empty
let cachedFetch (fetch: string -> Async<Downloader.DownloadResult>) =
    fun url ->
        async {
                do! sem.WaitAsync() |> Async.AwaitTask
                match Map.tryFind url cache with
                |   Some res ->
                    sem.Release() |> ignore
                    sprintf "cache hit: %s" url |> traceInfo
                    return res
                |   None ->
                    let! fetched = fetch url
                    sprintf "downloaded: %s" url |> traceInfo
                    cache <- Map.add url fetched cache
                    sprintf "cache size: %d" cache.Count |> traceInfo
                    sem.Release() |> ignore
                    return fetched

        }

type Pet911RealCrawling() =
    let tempDir = Path.Combine(Path.GetTempPath(),Path.GetRandomFileName())

    let downloadingAgent =
        let fetchUrl = Downloader.httpDownload userAgent 60000
        new Downloader.Agent(1, Downloader.defaultDownloaderSettings, fetchUrl)
    let downloadResource (desc:RemoteResourseDescriptor) = (cachedFetch downloadingAgent.Download) desc.url
    
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
                ID = "rf468348/1628158124610bb8ac4a6e25.22661272.webp"
                url= "https://cdn.pet911.ru/thumb_Pet_1628158124610bb8ac4a6e25.22661272.webp"
            }
            let! agent =
                constructPet911ImageProcessor tempDir downloadResource

            let! _ = agent.Process(descr);
            sprintf "shutting down" |> traceInfo
            do! agent.Shutdown()
            sprintf "shut down" |> traceInfo

            Assert.True(File.Exists(Path.Combine(tempDir,"rf468348","1628158124610bb8ac4a6e25.22661272.webp")))
        }

    [<Fact>]
    member _.``Acquiring inexistent photo is reported`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rf468348/1628158124610bb8ac4a6e25.00000000.webp"
                url= "https://cdn.pet911.ru/thumb_Pet_1628158124610bb8ac4a6e25.00000000.webp"
            }
            
            let! agent =
                constructPet911ImageProcessor tempDir downloadResource

            let! result = agent.Process(descr);
            do! agent.Shutdown()

            match result with
            |   Error er -> Assert.True(false, sprintf "supposed to get successful result: %s" er)
            |   Ok downloaded ->
                match downloaded with
                |   Missing _ -> Assert.True(true)
                |   Processed _ -> Assert.True(false, "supposed to get Missing result")
        }

    [<Fact>]
    member _.``Inexistent photo info persists`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rf468348/1628158124610bb8ac4a6e25.00000000.webp"
                url= "https://cdn.pet911.ru/thumb_Pet_1628158124610bb8ac4a6e25.00000000.webp"
            }

            let! agent =
                constructPet911ImageProcessor tempDir downloadResource

            let! result = agent.Process(descr);
            do! agent.Shutdown()

            match result with
            |   Error er -> Assert.True(false, sprintf "supposed to get successful result: %s" er)
            |   Ok downloaded ->
                match downloaded with
                |   Missing _ -> 
                    let filepath = Path.Combine(tempDir,missingImagesFilename)
                    Assert.True(File.Exists filepath)
                    let! lines = File.ReadAllLinesAsync(filepath) |> Async.AwaitTask
                    Assert.Equal(1,lines.Length)
                    Assert.Equal("rf468348/1628158124610bb8ac4a6e25.00000000.webp",lines.[0])
                |   Processed _ -> Assert.True(false, "supposed to get Missing result")
        }

    [<Fact>]
    member _.``Acquiring inexistent photo does not create file`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rf468348/1628158124610bb8ac4a6e25.22661272.webp"
                url= "https://cdn.pet911.ru/thumb_Pet_1628158124610bb8ac4a6e25.00000000.webp"
            }

            let! agent =
                constructPet911ImageProcessor tempDir downloadResource

            let! _ = agent.Process(descr);
            do! agent.Shutdown()

            Assert.False(File.Exists(Path.Combine(tempDir,"rf468348","1628158124610bb8ac4a6e25.22661272.webp")))
        }

    [<Fact>]
    member _.``Photo mime is written`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rf468348/1628158124610bb8ac4a6e25.22661272.webp"
                url= "https://cdn.pet911.ru/thumb_Pet_1628158124610bb8ac4a6e25.22661272.webp"
            }
            let! agent =
                constructPet911ImageProcessor tempDir downloadResource

            let! _ = agent.Process(descr);
            do! agent.Shutdown()

            let! mime = File.ReadAllTextAsync(Path.Combine(tempDir,"rf468348","1628158124610bb8ac4a6e25.22661272.webp.mime")) |> Async.AwaitTask
            Assert.Equal("image/webp",mime)
        }

    [<Fact>]
    member _.``Photo is checked`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rf468348/1628158124610bb8ac4a6e25.22661272.webp"
                url= "https://cdn.pet911.ru/thumb_Pet_1628158124610bb8ac4a6e25.22661272.webp"
            }

            let! agent =
                constructPet911ImageProcessor tempDir downloadResource

            let! result = agent.Process(descr);
            do! agent.Shutdown()

            Assert.False(hasFailed(result))
        }

    [<Fact>]
    member _.``Card is saved`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rf468348"
                url= "https://pet911.ru/%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0/%D0%BD%D0%B0%D0%B9%D0%B4%D0%B5%D0%BD%D0%B0/%D1%81%D0%BE%D0%B1%D0%B0%D0%BA%D0%B0/rf468348"
            }

            let! agent =
                constructPet911CardProcessor tempDir downloadResource

            let! result = agent.Process(descr);
            do! agent.Shutdown()
            
            Assert.False(hasFailed(result), "fetch is supposed to succeed")

            let path = Path.Combine(tempDir,"rf468348",cardFilename)

            Assert.True(File.Exists(path),sprintf "File %s does not exist" path)
        }

    [<Fact>]
    member _.``Card semantics extracted`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rf468348"
                url= "https://pet911.ru/%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0/%D0%BD%D0%B0%D0%B9%D0%B4%D0%B5%D0%BD%D0%B0/%D1%81%D0%BE%D0%B1%D0%B0%D0%BA%D0%B0/rf468348"
            }

            let! agent =
                constructPet911CardProcessor tempDir downloadResource

            let! result = agent.Process(descr);
            do! agent.Shutdown()
            
            match result with
            |   Error er -> Assert.False(true,er)
            |   Ok(check) ->
                match check with
                |   Missing -> Assert.True(false, "Card is missing while supposed to be there")
                |   Processed card ->
                    Assert.Equal("rf468348", card.id)
                    Assert.Equal(Species.dog, card.animal)
                    Assert.Equal(Some("Лилия"),card.author.name)
                    Assert.Equal("Московский Кремль и Красная Площадь, Дворцовая площадь, 19, Тверской район, Москва, Центральный федеральный округ, 103073, Россия",card.address)
                    Assert.Equal("Найден Хаски кобель, совсем молодой- около года. Серо- белый с голубыми глазами. На шее тонкий ошейник . Территориально пос. Мосрентген СНТ Дудкино-1".Replace("\n"," ").Replace("\r",""),card.description.Replace("\n"," ").Replace("\r",""))
                    Assert.Equal(Some(55.75581400), card.latitude)
                    Assert.Equal(Some(37.61763500), card.longitude)
                    Assert.Equal(System.DateTime(2022,8,4),card.date)
                    Assert.Equal(Sex.male, card.sex)
                    Assert.Equal(EventType.found, card.``type``)
                    Assert.Contains({url="https://cdn.pet911.ru/thumb_Pet_1628158124610bb8ac4a6e25.22661272.webp";ID="rf468348/thumb_Pet_1628158124610bb8ac4a6e25.22661272.webp"},card.photos)
        }

    [<Fact>]
    member _.``Card semantics extracted 2`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rl538274"
                url= "https://pet911.ru/%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0/%D0%BF%D1%80%D0%BE%D0%BF%D0%B0%D0%BB%D0%B0/%D0%BA%D0%BE%D1%88%D0%BA%D0%B0/rl538274"
            }

            let! agent =
                constructPet911CardProcessor tempDir downloadResource

            let! result = agent.Process(descr);
            do! agent.Shutdown()
            
            match result with
            |   Error er -> Assert.False(true,er)
            |   Ok(check) ->
                match check with
                |   Missing -> Assert.True(false, "Card is missing while supposed to be there")
                |   Processed card ->
                    Assert.Equal(Species.cat, card.animal)
        }

    [<Fact>]
    member _.``Missing card reported`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rl468348"
                url= "https://pet911.ru/%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0/%D0%BD%D0%B0%D0%B9%D0%B4%D0%B5%D0%BD%D0%B0/%D1%81%D0%BE%D0%B1%D0%B0%D0%BA%D0%B0/rl468348"
            }

            let! agent =
                constructPet911CardProcessor tempDir downloadResource

            let! result = agent.Process(descr);
            do! agent.Shutdown()
            
            match result with
            |   Error er -> Assert.False(true,er)
            |   Ok(check) ->
                match check with
                |   Missing -> Assert.True(true)
                |   Processed _ ->
                    Assert.True(false, "Card is present while supposed to be missing")
        }

    [<Fact>]
    member _.``Missing card info persisted`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rl468348"
                url= "https://pet911.ru/%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0/%D0%BD%D0%B0%D0%B9%D0%B4%D0%B5%D0%BD%D0%B0/%D1%81%D0%BE%D0%B1%D0%B0%D0%BA%D0%B0/rl468348"
            }

            let! agent =
                constructPet911CardProcessor tempDir downloadResource

            let! result = agent.Process(descr);
            do! agent.Shutdown()
            
            match result with
            |   Error er -> Assert.False(true,er)
            |   Ok(check) ->
                match check with
                |   Missing ->
                    let filePath = Path.Combine(tempDir, missingCardsFilename)
                    Assert.True(File.Exists filePath)
                    let! lines = File.ReadAllLinesAsync(filePath) |> Async.AwaitTask
                    Assert.Equal(1, lines.Length)
                    Assert.Equal("rl468348",lines.[0])
                |   Processed _ ->
                    Assert.True(false, "Card is present while supposed to be missing")
        }