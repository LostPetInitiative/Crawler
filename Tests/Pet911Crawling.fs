module Crawlers.Pet911

open System
open System.IO
open Xunit

open Kashtanka
open Kashtanka.Common
open Kashtanka.Crawler
open Kashtanka.SemanticTypes

open Kashtanka.CrawlerPet911
open System.Threading

let userAgent = "KashtankaTestRunner/0.0.1"

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
                ID = "rl476712/162560784360e4cea36deb30.11666472.jpeg"
                url= "https://pet911.ru/upload/Pet_thumb_162560784360e4cea36deb30.11666472.jpeg"
            }
            let! agent =
                constructPet911ImageProcessor tempDir downloadResource

            let! _ = agent.Process(descr);
            sprintf "shutting down" |> traceInfo
            do! agent.Shutdown()
            sprintf "shut down" |> traceInfo

            Assert.True(File.Exists(Path.Combine(tempDir,"rl476712","162560784360e4cea36deb30.11666472.jpeg")))
        }

    [<Fact>]
    member _.``Acquiring inexistent photo is reported`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rf476712/162560784360e4cea36deb30.00000000.jpeg"
                url= "https://pet911.ru/upload/Pet_thumb_162560784360e4cea36deb30.00000000.jpeg"
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
                ID = "rf476712/162560784360e4cea36deb30.00000000.jpeg"
                url= "https://pet911.ru/upload/Pet_thumb_162560784360e4cea36deb30.00000000.jpeg"
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
                    Assert.Equal("rf476712/162560784360e4cea36deb30.00000000.jpeg",lines.[0])
                |   Processed _ -> Assert.True(false, "supposed to get Missing result")
        }

    [<Fact>]
    member _.``Acquiring inexistent photo does not create file`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rf476712/162560784360e4cea36deb30.11666472.jpeg"
                url= "https://pet911.ru/upload/Pet_thumb_162560784360e4cea36deb30.00000000.jpeg"
            }

            let! agent =
                constructPet911ImageProcessor tempDir downloadResource

            let! _ = agent.Process(descr);
            do! agent.Shutdown()

            Assert.False(File.Exists(Path.Combine(tempDir,"rl476712","162560784360e4cea36deb30.11666472.jpeg")))
        }

    [<Fact>]
    member _.``Photo mime is written`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rl476712/162560784360e4cea36deb30.11666472.jpeg"
                url= "https://pet911.ru/upload/Pet_thumb_162560784360e4cea36deb30.11666472.jpeg"
            }
            let! agent =
                constructPet911ImageProcessor tempDir downloadResource

            let! _ = agent.Process(descr);
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
                ID = "rl476712"
                url= "https://pet911.ru/%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0/%D0%BF%D1%80%D0%BE%D0%BF%D0%B0%D0%BB%D0%B0/%D0%BA%D0%BE%D1%88%D0%BA%D0%B0/rl476712"
            }

            let! agent =
                constructPet911CardProcessor tempDir downloadResource

            let! result = agent.Process(descr);
            do! agent.Shutdown()
            
            Assert.True(hasFailed(result))

            Assert.True(File.Exists(Path.Combine(tempDir,"rl476712",CrawlerPet911.cardFilename)))
        }

    [<Fact>]
    member _.``Card semantics extracted`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rf492825"
                url= "https://pet911.ru/%D0%9D%D0%B8%D0%B6%D0%BD%D0%B8%D0%B9-%D0%9D%D0%BE%D0%B2%D0%B3%D0%BE%D1%80%D0%BE%D0%B4/%D0%BD%D0%B0%D0%B9%D0%B4%D0%B5%D0%BD%D0%B0/%D1%81%D0%BE%D0%B1%D0%B0%D0%BA%D0%B0/rf492825"
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
                    Assert.Equal("rf492825", card.id)
                    Assert.Equal(Species.dog, card.animal)
                    Assert.Equal("Екатерина",card.author.name)
                    Assert.Equal("Вернягово, городской округ Бор, Нижегородская область, Приволжский федеральный округ, 606485, Россия",card.address)
                    Assert.Equal("Нижний Новгород и область! 12 августа на борской трассе неподалеку от поворота к деревне Вернягово найдена рыжая собака (взрослый кобель) в коричневом кожаном ошейнике без адресника. Продолжаем поиск хозяев! Если не найдутся прежние, готовы отдать в новые заботливые ручки. В идеале в частный дом, т.к. пёс не привыкший к содержанию в квартире. Пёсель добрый, контактный, любит ласку. Также дружелюбен к другим собакам, если те сами не проявляют агрессию. Активный, словно шило в попе. Любит поиграть с мячиком, побегать с палкой, погрызть игрушку. Тел. 89101015049".Replace("\n"," ").Replace("\r",""),card.description.Replace("\n"," ").Replace("\r",""))
                    Assert.Equal(Some(56.28750000), card.latitude)
                    Assert.Equal(Some(44.31250000), card.longitude)
                    Assert.Equal(System.DateTime(2021,8,12),card.date)
                    Assert.Equal(Sex.male, card.sex)
                    Assert.Equal(EventType.found, card.``type``)
                    Assert.Contains({url="https://pet911.ru/upload/Pet_thumb_163492926461730a70237913.59627594.jpeg";ID="rf492825/163492926461730a70237913.59627594.jpeg"},card.photos)
                    Assert.Contains({url="https://pet911.ru/upload/Pet_thumb_163492933561730ab74aae92.23740926.jpeg";ID="rf492825/163492933561730ab74aae92.23740926.jpeg"},card.photos)
                    Assert.Contains({url="https://pet911.ru/upload/Pet_thumb_163492941061730b0281fd52.90613683.jpeg";ID="rf492825/163492941061730b0281fd52.90613683.jpeg"},card.photos)
        }

    [<Fact>]
    member _.``Missing card reported`` () =
        async {
            let descr:RemoteResourseDescriptor = {
                ID = "rl492825"
                url= "https://pet911.ru/%D0%9D%D0%B8%D0%B6%D0%BD%D0%B8%D0%B9-%D0%9D%D0%BE%D0%B2%D0%B3%D0%BE%D1%80%D0%BE%D0%B4/%D0%BD%D0%B0%D0%B9%D0%B4%D0%B5%D0%BD%D0%B0/%D1%81%D0%BE%D0%B1%D0%B0%D0%BA%D0%B0/rl492825"
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
                ID = "rl492825"
                url= "https://pet911.ru/%D0%9D%D0%B8%D0%B6%D0%BD%D0%B8%D0%B9-%D0%9D%D0%BE%D0%B2%D0%B3%D0%BE%D1%80%D0%BE%D0%B4/%D0%BD%D0%B0%D0%B9%D0%B4%D0%B5%D0%BD%D0%B0/%D1%81%D0%BE%D0%B1%D0%B0%D0%BA%D0%B0/rl492825"
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
                    Assert.Equal("rl492825",lines.[0])
                |   Processed _ ->
                    Assert.True(false, "Card is present while supposed to be missing")
        }