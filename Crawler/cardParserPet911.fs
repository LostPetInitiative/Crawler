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