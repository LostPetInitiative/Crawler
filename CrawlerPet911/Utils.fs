module Kashtanka.pet911.Utils

open Kashtanka.Crawler
open Kashtanka.SemanticTypes
open Newtonsoft.Json.Linq
open FSharp.Data
open System

let cardIDsFromRange firstCard lastCard =
    seq {
        for i in firstCard..lastCard do
            yield sprintf "rf%d" i;
            yield sprintf "rl%d" i;
        }

let cardIDToURL (cardID:string) =
    System.String.Format("""https://pet911.ru/%D0%A5%D0%B0%D0%B1%D0%B0%D1%80%D0%BE%D0%B2%D1%81%D0%BA/%D0%BD%D0%B0%D0%B9%D0%B4%D0%B5%D0%BD%D0%B0/%D1%81%D0%BE%D0%B1%D0%B0%D0%BA%D0%B0/{0}""",cardID)

let cardIDtoDescriptor cardID : RemoteResourseDescriptor = 
    {
        ID = cardID;
        url = cardIDToURL cardID
    }

/// ID = [cardID]/[photoID.ext]
let parsePhotoId (id:string) =
    let parts = id.Split([|'/'|])
    match parts with
    | [| cardId; photoId |] -> Some(cardId,photoId)
    |   _ -> None


let cardToPipelineJSON (card:PetCard) =
    let pet = new JObject()

    pet.Add("art",JValue(card.id))
    
    let species =
        match card.animal with
        |   Species.dog -> Some "1"
        |   Species.cat -> Some "2"
        |   _ -> Some "0"
    if species.IsSome then
        pet.Add("animal", JValue(species.Value))

    let sex =
        match card.sex with
        |   Sex.male -> Some "2"
        |   Sex.female -> Some "3"
        |   _ -> Some "0"
    if sex.IsSome then
        pet.Add("sex", JValue(sex.Value))

    pet.Add("address", JValue(if String.IsNullOrEmpty(card.address) then "" else card.address))

    if card.latitude.IsSome then
        pet.Add("latitude",JValue(sprintf "%f" card.latitude.Value))

    if card.longitude.IsSome then
        pet.Add("longitude",JValue(sprintf "%f" card.longitude.Value))

    pet.Add("date",JValue(sprintf "%d" (int (card.date - DateTime.UnixEpoch).TotalSeconds)))

    let eventType =
        match card.``type`` with
        |   EventType.found -> Some "2"
        |   EventType.lost -> Some "1"
        |   _ -> None
    if eventType.IsSome then
        pet.Add("type",JValue(eventType.Value))

    pet.Add("description", JValue(card.description))

    let author = JObject()
    if card.author.name.IsSome then
        author.Add("username",JValue(card.author.name.Value))
    else
        author.Add("username",JValue(""))
    if card.author.phone.IsSome then
        author.Add("phone",JValue(card.author.phone.Value))
    if card.author.email.IsSome then
        author.Add("email",JValue(card.author.email.Value))
    pet.Add("author", author);

    let photos = JArray()
    let descriptorToPhoto (descriptor:RemoteResourseDescriptor) =
        let res = JObject()
        let filenameWithExt = snd (parsePhotoId descriptor.ID).Value
        let baseName = System.IO.Path.GetFileNameWithoutExtension(filenameWithExt)
        res.Add("id",JValue(baseName))
        res
    card.photos |> Seq.iter (fun x -> photos.Add(descriptorToPhoto x))
    pet.Add("photos",photos)

    let res = JObject()
    res.Add("pet",pet)
    res

let pingPipeline (newIds:string seq) =
    async {
        let jsonBody = sprintf """{ "cardIds": [%s] }""" (String.Join(',', newIds |> Seq.map (fun x -> sprintf "\"%s\"" x)))
        try
            let! pingResult =
                Http.AsyncRequest(
                    "http://127.0.0.1:5001/",
                    headers = [
                                HttpRequestHeaders.ContentType HttpContentTypes.Json;
                                // HttpRequestHeaders.Accept HttpContentTypes.Json;
                                // HttpRequestHeaders.UserAgent agentName;
                                // HttpRequestHeaders.Origin urlPrefix
                                ],
                    httpMethod = "POST",
                    body = TextRequest jsonBody, silentHttpErrors = true, timeout = 10000)
            match pingResult.StatusCode with
            |   201 ->
                return Ok()
            |   code ->
                return Error(sprintf "Failed to notify processing pipeline. Http code %d" code)                    
        with
            |   :? System.Net.WebException as ex->
                return Error(sprintf "Failed to notify processing pipeline: %A" ex)
    }