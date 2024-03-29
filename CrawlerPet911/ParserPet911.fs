﻿module Kashtanka.pet911.Parsers

open HtmlAgilityPack
open Kashtanka.SemanticTypes
open Kashtanka.Crawler

let hostUrl = "https://pet911.ru"

let getCardId (htmlDoc:HtmlDocument) : Result<string,string> =        
    let idNodes = htmlDoc.DocumentNode.SelectNodes("//div[@class='card']//div[@class='card-information']/div[@class='card-info'][div='Номер объявления']/div[@class='card-info__value']");
    if idNodes = null then
        Error("Can't find cardID element")
    elif idNodes.Count <> 1 then Error(sprintf "Found %d cardID instead of 1" idNodes.Count)
    else
        let node = idNodes.[0]
        Ok(node.InnerText.Trim())


let getAnimalSpecies (htmlDoc:HtmlDocument) : Result<Species,string> =
    let breadcrumbNodes = htmlDoc.DocumentNode.SelectNodes("//div[contains(@class,'breadcrumbs')]//a[contains(@class,'breadcrumbs__item')]");
    if breadcrumbNodes = null then
        Error "Can't find breadcrumb nodes"
    else
        let interHtmlTexts =
            breadcrumbNodes
            |> Seq.map (fun node -> node.InnerHtml)
            |> Seq.toList

        if List.exists (fun (t:string) -> t.Contains("собаки", System.StringComparison.OrdinalIgnoreCase)) interHtmlTexts then Ok(Species.dog)
        elif List.exists (fun (t:string) -> t.Contains("кошки", System.StringComparison.OrdinalIgnoreCase)) interHtmlTexts then Ok(Species.cat)
        else Error("Can't detect species")

let getPhotoUrls (htmlDoc:HtmlDocument) : Result<string[], string> =
    let photoNodes = htmlDoc.DocumentNode.SelectNodes("//div[@class='card']//div[@class='swiper-wrapper']//a[contains(@class,'js-card-slide')]/img")
    if photoNodes = null then
        Ok(Array.empty)
    else
        let hrefs =
            photoNodes
            |> Seq.map (fun node -> node.Attributes.["src"].Value)
            |> Seq.filter (fun x -> not(x.StartsWith("https://pet911.ru/img/no-photo/"))) // missing photo stub
            |> Array.ofSeq
        Ok(hrefs)
        
let getEventTimeUTC (htmlDoc:HtmlDocument) : Result<System.DateTime, string> =
    let dateNodes = htmlDoc.DocumentNode.SelectNodes(@"//div[@class='card']//div[@class='card-information']/div[@class='card-info'][contains(div,'Найден') or contains(div,'Пропал')]/div[@class='card-info__value']")
    if dateNodes = null then
        Error "Can't find event time element"
    elif dateNodes.Count <> 1 then
        Error(sprintf "Expected 1 date elements, found %d" dateNodes.Count) //rf518209 Найден(а)
    else
        let eventDateNode = dateNodes |> Seq.exactlyOne
        let text = eventDateNode.InnerText.ToLower().Trim()
        let dateText = text.Substring(text.Length - 10)
        let couldParse, date = System.DateTime.TryParseExact(dateText,"dd.MM.yyyy",System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeUniversal ||| System.Globalization.DateTimeStyles.AdjustToUniversal)
        if couldParse then
            Ok date
        else
            Error "Could not parse event date"

let getAuthorName (htmlDoc:HtmlDocument) : Result<string option,string> =
    let authorNodes = htmlDoc.DocumentNode.SelectNodes("//div[@class='card']//div[@class='card-information']/div[@class='card-info'][contains(div,'Имя хозяина') or contains(div,'Имя нашедшего')]/div[@class='card-info__value']")
    if authorNodes = null then
        Ok None
    elif authorNodes.Count <> 1 then
        Error(sprintf "Expected single author element, found %d" authorNodes.Count)
    else
        Ok(Some(authorNodes.[0].InnerText.Trim()))

let getAuthorMessage (htmlDoc:HtmlDocument) : Result<string,string> =
    let messageNodes = htmlDoc.DocumentNode.SelectNodes("//div[@class='card']//div[@class='card__content']//div[contains(@class, 'card__descr')]/p")
    if messageNodes = null then
        Error "Can't find message element"
    elif messageNodes.Count <> 1 then
        Error(sprintf "Expected single description element, but got %d" messageNodes.Count)
    else
        Ok(messageNodes.[0].InnerText.Trim())

let getEventAddress (htmlDoc:HtmlDocument) : Result<string, string> =
    let addressNodes = htmlDoc.DocumentNode.SelectNodes("//div[@class='card']//div[contains(@class,'card-map__address')]")
    if addressNodes = null then
        Error "Can't find address element"
    elif addressNodes.Count <> 1 then
        Error(sprintf "Expected single address element, but got %d" addressNodes.Count)
    else
        Ok(addressNodes.[0].InnerText.Trim())

let getAnimalSex (htmlDoc:HtmlDocument) : Result<Sex, string> =
    let sexNodes = htmlDoc.DocumentNode.SelectNodes("//div[@class='card']//div[@class='card-information']/div[@class='card-info'][div='Пол питомца']/div[@class='card-info__value']")
    if sexNodes = null then
        Ok Sex.unknown
    elif sexNodes.Count <> 1 then
        Error(sprintf "Expected single animal sex element, but got %d" sexNodes.Count)
    else
        let sex =
            match sexNodes.[0].InnerText.Trim().ToLowerInvariant() with
            |   "мужской" -> Ok Sex.male
            |   "женский" -> Ok Sex.female
            |   s -> Error (sprintf "Unexpected sex value %s" s)
        sex

let getEventType (htmlDoc:HtmlDocument) : Result<EventType, string> =
    let breadcrumbNodes = htmlDoc.DocumentNode.SelectNodes("//div[contains(@class,'breadcrumbs')]//a[contains(@class,'breadcrumbs__item')]");
    if breadcrumbNodes = null then
        Error "Can't find breadcrumb nodes"
    else
        let interHtmlTexts =
            breadcrumbNodes
            |> Seq.map (fun node -> node.InnerHtml)
            |> Seq.toList

        if List.exists (fun (t:string) -> t.Contains("найденные", System.StringComparison.OrdinalIgnoreCase)) interHtmlTexts then Ok(EventType.found)
        elif List.exists (fun (t:string) -> t.Contains("пропавшие", System.StringComparison.OrdinalIgnoreCase)) interHtmlTexts then Ok(EventType.lost)
        else Error("Can't detect event type")

let getEventCoords (htmlDoc:string) : Result<float*float, string> =
    match htmlDoc with
    | Kashtanka.Common.InterpretedMatch @"initMap\s*\((.|\n)*\{\s*lat\s*:\s*(?'lat'[\d\.]+)\s*,\s*lng\s*:\s*(?'lon'[\d\.]+)\s*\}" [_;_;latGrp; lonGrp] ->
        match System.Double.TryParse(latGrp.Value),System.Double.TryParse(lonGrp.Value) with
        |   (true,lat),(true,lon) ->
            Ok(lat,lon) 
        |   _ -> Error "Can't parse lat/lon"
    | _ ->  Error "Regex did not find the lat/lon"

let getCatalogCards (htmlDoc:HtmlDocument) : Result<RemoteResourseDescriptor[],string> = 
    let cardRefNodes = htmlDoc.DocumentNode.SelectNodes("//div[contains(@class,'catalog-item')]//a[@class='catalog-item__thumb']")
    let idFromUrl (url:string) =
        let idx = url.LastIndexOf('/')
        url.Substring(idx+1)
    let hrefs =
        cardRefNodes
        |> Seq.map (fun node -> node.GetAttributeValue("href","NOT_FOUND"))
        |> Seq.distinct
        |> Seq.map (fun x -> {ID = idFromUrl x; url = sprintf "%s%s" hostUrl x})
        |> Seq.toArray
    Ok(hrefs)

/// Returns photoID (incl. extension)
let getPhotoId (photoUrl: System.Uri) =
    let photoUrlStr = photoUrl.ToString()
    let lastSlashPos = photoUrlStr.LastIndexOf '/'
    if lastSlashPos = -1 then
        Error(sprintf "Did not find slash: %O" photoUrl)
    else
        Ok(photoUrlStr.Substring(lastSlashPos+1))