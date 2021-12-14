module Kashtanka.NewCards

open Crawler
open Downloader

let urlsToQueryBase = [|
    "https://pet911.ru/catalog?PetsSearch[animal]=2&PetsSearch[type]=1"; // & page=2 ...
    "https://pet911.ru/catalog?PetsSearch[animal]=1&PetsSearch[type]=1";
    "https://pet911.ru/catalog?PetsSearch[animal]=2&PetsSearch[type]=2";
    "https://pet911.ru/catalog?PetsSearch[animal]=1&PetsSearch[type]=2";
|]

let getNewCards (knownToLookFor: Set<int> option) (download: RemoteResourseDescriptor -> Async<Result<RemoteResourseLookup,string>>) =
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
            let! lookups = descriptors |> Seq.map download |> Async.Parallel
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
