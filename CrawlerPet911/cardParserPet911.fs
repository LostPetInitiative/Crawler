module Kashtanka.Parsers.pet911

open HtmlAgilityPack
open Kashtanka.SemanticTypes

let photoUrlPrefix = "https://pet911.ru/upload/Pet_thumb_"

let getCardId (htmlDoc:HtmlDocument) : Result<string,string> =        
    let idNodes = htmlDoc.DocumentNode.SelectNodes("//div[@class='p-art']/span[@class='text']");
    if idNodes = null then
        Error("Can't find cardID element")
    elif idNodes.Count <> 1 then Error(sprintf "Found %d cardID instead of 1" idNodes.Count)
    else
        let node = idNodes.[0]
        Ok(node.InnerText.Trim())


let getAnimalSpecies (htmlDoc:HtmlDocument) : Result<Species,string> =
    let speciesNodes = htmlDoc.DocumentNode.SelectNodes("//div[@class='only-mobile']/div[@class='p-animal']");
    if speciesNodes = null then
        Error "Can't find species node"
    elif speciesNodes = null or speciesNodes.Count <> 1 then Error(sprintf "Found %d species tags instead of 1" speciesNodes.Count)
    else
        let node = speciesNodes.[0]
        match node.InnerText.Trim().ToLowerInvariant() with
        |   "кошка" -> Ok(Species.cat)
        |   "собака" -> Ok(Species.dog)
        |   speciesStr -> Error(sprintf "Unknown species \"%s\"" speciesStr)

let getPhotoUrls (htmlDoc:HtmlDocument) : Result<string[], string> =
    let photoNodes = htmlDoc.DocumentNode.SelectNodes("//a[@data-lightbox='pet']")
    if photoNodes = null then
        Error ("Can't find photo elements")
    else
        let hrefs = photoNodes |> Seq.map (fun node -> node.Attributes.["href"].Value) |> Array.ofSeq
        if hrefs |> Seq.forall (fun (href:string) -> href.StartsWith("/upload/Pet_")) then
            Ok(hrefs |> Array.map (fun href -> sprintf "%s%s" photoUrlPrefix (href.Substring("/upload/Pet_".Length))))
        else
            Error(sprintf "One of the photo URLs is unexpected: %s" (hrefs |> Seq.find (fun x -> not(x.StartsWith("/upload/Pet_")))))

let getEventTimeUTC (htmlDoc:HtmlDocument) : Result<System.DateTime, string> =
    let dateNodes = htmlDoc.DocumentNode.SelectNodes("//section[@id='view-pet']//div[@class='only-mobile']/div[@class='p-date']")
    if dateNodes = null then
        Error "Can't find event time elemet"
    elif dateNodes.Count <> 2 then
        Error(sprintf "Expected 2 date elements, found %d" dateNodes.Count)
    else
        let eventDateNode = dateNodes |> Seq.filter (fun x -> x.InnerText.ToLowerInvariant().Contains("дата")) |> Seq.exactlyOne
        let text = eventDateNode.InnerText.ToLower().Trim()
        let dateText = text.Substring(text.Length - 10)
        let couldParse, date = System.DateTime.TryParseExact(dateText,"dd.MM.yyyy",System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None)
        if couldParse then
            Ok date
        else
            Error "Could not parse event date"

let getAuthorName (htmlDoc:HtmlDocument) : Result<string,string> =
    let authorNodes = htmlDoc.DocumentNode.SelectNodes("//section[@id='view-pet']//div[@class='p-author']/span[@class='text']")
    if authorNodes = null then
        Error "Can't find author element"
    elif authorNodes.Count <> 1 then
        Error(sprintf "Expected single author element, found %d" authorNodes.Count)
    else
        Ok(authorNodes.[0].InnerText.Trim())

let getAuthorMessage (htmlDoc:HtmlDocument) : Result<string,string> =
    let messageNodes = htmlDoc.DocumentNode.SelectNodes("//section[@id='view-pet']//div[@class='p-description']")
    if messageNodes = null then
        Error "Can't find message element"
    elif messageNodes.Count <> 1 then
        Error(sprintf "Expected single description element, but got %d" messageNodes.Count)
    else
        Ok(messageNodes.[0].InnerText.Trim())

let getEventAddress (htmlDoc:HtmlDocument) : Result<string, string> =
    let addressNodes = htmlDoc.DocumentNode.SelectNodes("//section[@id='view-pet']//div[@class='p-address']/span[@class='text']")
    if addressNodes = null then
        Error "Can't find address element"
    elif addressNodes.Count <> 1 then
        Error(sprintf "Expected single address element, but got %d" addressNodes.Count)
    else
        Ok(addressNodes.[0].InnerText.Trim())

let getAnimalSex (htmlDoc:HtmlDocument) : Result<Sex, string> =
    let sexNodes = htmlDoc.DocumentNode.SelectNodes("//section[@id='view-pet']//div[@class='p-sex']/span[@class='text']")
    if sexNodes = null then
        Error "Can't find sex element"
    elif sexNodes.Count <> 1 then
        Error(sprintf "Expected single animal sex element, but got %d" sexNodes.Count)
    else
        let sex =
            match sexNodes.[0].InnerText.Trim().ToLowerInvariant() with
            |   "м" -> Sex.male
            |   "ж" -> Sex.female
            |   _ -> Sex.unknown
        Ok sex

let getEventType (htmlDoc:HtmlDocument) : Result<EventType, string> =
    let dateNodes = htmlDoc.DocumentNode.SelectNodes("//section[@id='view-pet']//div[@class='only-mobile']/div[@class='p-date']")
    if dateNodes = null then
        Error "Can't find event date element"
    elif dateNodes.Count <> 2 then
        Error(sprintf "Expected 2 date elements, found %d" dateNodes.Count)
    else
        let eventDateNode = dateNodes |> Seq.filter (fun x -> x.InnerText.ToLowerInvariant().Contains("дата")) |> Seq.exactlyOne
        let text = eventDateNode.InnerText.ToLower().Trim()
        if text.Contains("пропажи") then
            Ok EventType.lost
        else
            if text.Contains("находки") then
                Ok EventType.found
            else
                Error (sprintf "Could not find event type keyword: \"%s\"" text)

let getEventCoords (htmlDoc:string) : Result<float*float, string> =
    match htmlDoc with
    | Kashtanka.Common.InterpretedMatch @"initMap\s*\(\s*\{\s*lat\s*:\s*([\d\.]+)\s*,\s*lng\s*:\s*([\d\.]+)\s*\}" [_; latGrp; lonGrp] ->
        match System.Double.TryParse(latGrp.Value),System.Double.TryParse(lonGrp.Value) with
        |   (true,lat),(true,lon) ->
            Ok(lat,lon) 
    | _ -> 
        Error "Regex did not find the lat/lon"