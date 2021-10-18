module Kashtanka.Parsers.pet911

open HtmlAgilityPack
open Kashtanka.SemanticTypes

let getCardId (htmlDoc:HtmlDocument) : Result<string,string> =        
    let idNodes = htmlDoc.DocumentNode.SelectNodes("//div[@class='p-art']/span[@class='text']");
    if idNodes.Count <> 1 then Error(sprintf "Found %d cardID instead of 1" idNodes.Count)
    else
        let node = idNodes.[0]
        Ok(node.InnerText.Trim())


let getAnimalSpecies (htmlDoc:HtmlDocument) : Result<Species,string> =
    let speciesNodes = htmlDoc.DocumentNode.SelectNodes("//div[@class='only-mobile']/div[@class='p-animal']");
    if speciesNodes.Count <> 1 then Error(sprintf "Found %d species tags instead of 1" speciesNodes.Count)
    else
        let node = speciesNodes.[0]
        match node.InnerText.Trim().ToLowerInvariant() with
        |   "кошка" -> Ok(Species.cat)
        |   "собака" -> Ok(Species.dog)
        |   speciesStr -> Error(sprintf "Unknown species \"%s\"" speciesStr)

let getPhotoUrls (htmlDoc:HtmlDocument) : Result<string[], string> =
    let photoNodes = htmlDoc.DocumentNode.SelectNodes("//a[@data-lightbox='pet']")
    let hrefs = photoNodes |> Seq.map (fun node -> node.Attributes.["href"].Value) |> Array.ofSeq
    if hrefs |> Seq.forall (fun (href:string) -> href.StartsWith("/upload/Pet_")) then
        Ok(hrefs |> Array.map (fun href -> sprintf "https://pet911.ru/upload/Pet_thumb_%s" (href.Substring("/upload/Pet_".Length))))
    else
        Error(sprintf "One of the photo URLs is unexpected: %s" (hrefs |> Seq.find (fun x -> not(x.StartsWith("/upload/Pet_")))))

let getEventTimeUTC (htmlDoc:HtmlDocument) : Result<System.DateTime, string> =
    let dateNodes = htmlDoc.DocumentNode.SelectNodes("//section[@id='view-pet']//div[@class='only-mobile']/div[@class='p-date']")
    if dateNodes.Count <> 2 then
        Error(sprintf "Expected 2 date elemenent, found %d" dateNodes.Count)
    else
        let eventDateNode = dateNodes |> Seq.filter (fun x -> x.InnerText.ToLowerInvariant().Contains("дата")) |> Seq.exactlyOne
        let text = eventDateNode.InnerText.ToLower().Trim()
        let dateText = text.Substring(text.Length - 10)
        let couldParse, date = System.DateTime.TryParseExact(dateText,"dd.MM.yyyy",System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None)
        if couldParse then
            Ok date
        else
            Error "Could not parse event date"