module Kashtanka.NewCards

open FSharp.Control
open Newtonsoft.Json.Linq
open Crawler
open Downloader

type FetchUrlType = System.Uri -> Async<DownloadResult>

let getNewCardsFromCatalog (knownToLookFor: Set<int> option) (download: System.Uri -> Async<Result<RemoteResourseLookup,string>>) =
    let urlsToQueryBase = [|
        "https://pet911.ru/catalog?PetsSearch[animal]=2&PetsSearch[type]=1"; // & page=2 ...
        "https://pet911.ru/catalog?PetsSearch[animal]=1&PetsSearch[type]=1";
        "https://pet911.ru/catalog?PetsSearch[animal]=2&PetsSearch[type]=2";
        "https://pet911.ru/catalog?PetsSearch[animal]=1&PetsSearch[type]=2";
    |]

    let getPageCardsDescriptors pageNum = 
        async {
            let descriptors =
                urlsToQueryBase
                |> Seq.mapi (fun idx urlBase -> {ID= sprintf "dummy_id_%d_%d" pageNum idx; url=sprintf "%s&page=%d" urlBase pageNum })
            let lookupResultToIds (lookupRes:Result<RemoteResourseLookup,string>) =
                match lookupRes with
                |   Error er -> Error er
                |   Ok lookup ->
                    match lookup with
                    |   Absent -> Error "Catalog page does not exist"
                    |   Downloaded downloaded ->
                        let text = downloadedFileToText (fst downloaded)
                        let doc = new HtmlAgilityPack.HtmlDocument()
                        doc.LoadHtml(text)
                        pet911.Parsers.getCatalogCards doc 
            let! lookups = descriptors |> Seq.map (fun x -> download (System.Uri x.url)) |> Async.Parallel
            let descriptors = lookups |> Seq.map lookupResultToIds
            match Common.allResults descriptors with
            |   Error er -> return Error (sprintf "Failed to parse one of the catalogs: %s" er)
            |   Ok descriptors ->
                return Ok(descriptors |> Seq.concat)
        }
        
    async {
        match knownToLookFor with
        |   None ->
            // we return single card with largest
            let! descriptorsRes = getPageCardsDescriptors 1
            return descriptorsRes |> Result.map (fun x -> x |> Seq.sortByDescending (fun x -> System.Int32.Parse(x.ID.Substring(2))) |> Seq.head |> Set.singleton)
        |   Some known ->
            // looking the catalog until we find what we already have
            let rec findLatest curPage accumulated =
                async {
                    match! getPageCardsDescriptors curPage with
                    |   Error er -> return Error (sprintf "Error while processing page %d of catalog: %s" curPage er)
                    |   Ok descSeq ->
                        let descSet = Set.ofSeq descSeq
                        let newSet = Seq.fold (fun s desc -> Set.add desc s) accumulated descSet
                        if not(Set.isEmpty (Set.intersect known (Set.map (fun x -> System.Int32.Parse(x.ID.Substring(2))) descSet))) then
                            // found returning everything including current page
                            return Ok(newSet)
                        else
                            // trying next page
                            return! findLatest (curPage + 1) newSet

                }
            return! findLatest 1 Set.empty
    }

let searchCardURLsBySubstring (fetchJson:FetchUrlType) substring =
    async {
        let! checkJsonRes = fetchJson (System.Uri(sprintf "https://pet911.ru/ajax/check-pet?art=%s" substring))
        match checkJsonRes with
        |   Error er -> return Error er
        |   Ok resp ->
            match resp with
            |   Absent -> return Error "Unexpected 404"
            |   Downloaded(d, _) ->
                match d with
                |   Binary _ -> return Error "Unexpected binary response"
                |   Text t ->
                    let jObject = JObject.Parse(t)
                    let data = jObject.GetValue("data")
                    if data = null then
                        return Ok Array.empty
                    else
                        let arts =
                            data.Children()
                            |> Seq.map (fun jtoken -> jtoken.Value<string>("url"))
                        let resArray = Array.ofSeq arts
                        return Ok(resArray)
    }

let verifyCardExists (fetchJson:FetchUrlType) num  =
    async {
        let numStr = sprintf "%d" num
        let! artsRes = searchCardURLsBySubstring fetchJson numStr
        match artsRes with
        |   Error e -> return Error e
        |   Ok arts -> return Ok (arts |> Seq.exists (fun x -> x.EndsWith numStr))
    }

let getNewCardsFromCheckAPI (knownToLookFor: Set<int> option) (download: FetchUrlType) lookAheadNumber =
    async {
        match knownToLookFor with
        |   None ->
            // fallback to catalog lookup
            return! getNewCardsFromCatalog None download
        |   Some knownIds ->
            // from largest to smallest
            // 1. look for the largest that still exists. Call it confirmedLargest
            let knownSorted = knownIds |> Seq.sortByDescending id
            let verificationSeq = knownSorted |> Seq.map (fun x -> x,(verifyCardExists download x))
            let! largestVerified =
                asyncSeq {
                    for num,verification in verificationSeq do
                        match! verification with
                        |   Error e ->
                            sprintf "Failed to verify existence of ad %d: %A" num e |> traceError
                            ()
                        |   Ok v ->
                            yield if v then Some(num) else None
                    }
                |> AsyncSeq.choose id
                |> AsyncSeq.tryFirst
            // 2. based on that, start to do lookahead probes for new cards through the search API
            //  we are looking at searching the prefix of the card ID (e.g. 12345 -> 1234). We do this for lastest knonwn carda and for lookahead card
            match largestVerified with
            |   None ->
                // did not find any known card. Fallback to catalog lookup
                return! getNewCardsFromCatalog None download
            |   Some largestVerifiedNum ->
                let largestVerifiedTens = largestVerifiedNum / 10
                let lookaheadTens = (largestVerifiedNum + lookAheadNumber) / 10
                let! found =
                    asyncSeq {
                        for tens in largestVerifiedTens .. lookaheadTens do
                            match! searchCardURLsBySubstring download (sprintf "%d" tens) with
                            |   Error e ->
                                sprintf "Failed to search cards by substring %d: %A" tens e |> traceError
                                ()
                            |   Ok arts ->
                                let nums = arts |> Seq.map (fun x -> let slashIdx = x.LastIndexOf '/' in { ID = x.Substring(slashIdx+1); url = x} )
                                for n in nums do
                                    yield n
                        }
                    |> AsyncSeq.fold (fun s v -> Set.add v s) Set.empty
                return Ok found

    }
