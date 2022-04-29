﻿module Parsers.Pet911

open System
open System.IO
open Xunit

open Kashtanka.Common
open Kashtanka.pet911.Parsers
open HtmlAgilityPack
open Kashtanka.SemanticTypes

let dataDir = "../../../../data/2022/"

let loadAndParseHtmlTestFile filename =
    async {
        let! text = IO.File.ReadAllTextAsync(Path.Combine(dataDir,filename)) |> Async.AwaitTask
        let doc = new HtmlDocument()
        doc.LoadHtml(text)
        return doc
    }

[<Fact>]
let ``Extract card id`` () =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rf518209.html.dump"
        let parseRes = getCardId doc
        Assert.Equal("rf518209",extractSuccessful(parseRes))
    }

[<Fact>]
let ``Extract cat species`` () =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl518787.html.dump"
        let parseRes = getAnimalSpecies doc
        Assert.Equal(Species.cat, extractSuccessful(parseRes))
    }

[<Fact>]
let ``Extract dog species`` () =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rf518209.html.dump"
        let parseRes = getAnimalSpecies doc
        Assert.Equal(Species.dog, extractSuccessful(parseRes))
    }


[<Fact>]
let ``Extract photo URLs`` () =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl518787.html.dump"
        let parseRes = getPhotoUrls doc
        Assert.Equal(7, extractSuccessful(parseRes).Length)
        Assert.Contains("https://cdn.pet911.ru/thumb_Pet_165095355762678d5570db96.27027701.webp",extractSuccessful(parseRes))
        Assert.Contains("https://cdn.pet911.ru/thumb_Pet_165095340062678cb83dea18.58046461.webp",extractSuccessful(parseRes))
        Assert.Contains("https://cdn.pet911.ru/thumb_Pet_165095343462678cda7583a1.69548470.webp",extractSuccessful(parseRes))
        Assert.Contains("https://cdn.pet911.ru/thumb_Pet_165095351562678d2bb44ab4.37666840.webp",extractSuccessful(parseRes))
        Assert.Contains("https://cdn.pet911.ru/thumb_Pet_165095351662678d2c27d440.01200981.webp",extractSuccessful(parseRes))
        Assert.Contains("https://cdn.pet911.ru/thumb_Pet_165095355562678d53ce94c6.03768364.webp",extractSuccessful(parseRes))
        Assert.Contains("https://cdn.pet911.ru/thumb_Pet_165095355662678d54a27803.69782174.webp",extractSuccessful(parseRes))
        

        
    }

[<Fact>]
let ``Card with no photos`` () =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl518678_no_photos.html.dump"
        let parseRes = getPhotoUrls doc
        Assert.Equal(0, extractSuccessful(parseRes).Length)        
    }

[<Fact(Skip="Needs update to match new website structure")>]
let ``Extract event time``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl476712.html.dump"
        let parseRes = getEventTimeUTC(doc) 
        Assert.Equal(System.DateTime(2021,6,26),extractSuccessful(parseRes))
    }

[<Fact>]
let ``Extract author name``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rf518209.html.dump"
        let authorRes = getAuthorName(doc) 
        Assert.Equal(Some("Максим"),extractSuccessful(authorRes))
    }

[<Fact>]
let ``Extract author name for lost card that is found``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl518948_lost_is_found.html.dump"
        let authorRes = getAuthorName(doc) 
        Assert.Equal(None,extractSuccessful(authorRes))
    }

[<Fact>]
let ``Extract author message``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rf518209.html.dump"
        let messageRes = getAuthorMessage(doc) 
        Assert.Equal("Нашли на улице, лежал с раненной лапой. Есть ошейник.", extractSuccessful(messageRes))
    }

[<Fact(Skip="Needs update to match new website structure")>]
let ``Extract event address``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl476712.html.dump"
        let messageRes = getEventAddress(doc) 
        Assert.Equal("11 к1, Чусовская улица, район Гольяново, Москва, Центральный федеральный округ, 107207, Россия", extractSuccessful(messageRes))
    }

[<Fact>]
let ``Extract animal sex``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rf518209.html.dump"
        let sexRes = getAnimalSex(doc) 
        Assert.Equal(Sex.male, extractSuccessful(sexRes))
    }

[<Fact(Skip="Needs update to match new website structure")>]
let ``Animal sex unknown``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rf494618_no_sex.html.dump"
        let sexRes = getAnimalSex(doc) 
        Assert.Equal(Sex.unknown, extractSuccessful(sexRes))
    }

[<Fact(Skip="Needs update to match new website structure")>]
let ``No author``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rf494610_no_author.html.dump"
        let authorName = getAuthorName(doc) 
        Assert.False(hasFailed(authorName))
        Assert.Equal(None,extractSuccessful(authorName))
    }

[<Fact(Skip="Needs update to match new website structure")>]
let ``Extract event type``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl476712.html.dump"
        let eventTypeRes = getEventType(doc) 
        Assert.Equal(EventType.lost, extractSuccessful(eventTypeRes))
    }

[<Fact(Skip="Needs update to match new website structure")>]
let ``Extract event coords``() =
    async {
        let! text = IO.File.ReadAllTextAsync(Path.Combine(dataDir,"petCard_rl476712.html.dump")) |> Async.AwaitTask
        match getEventCoords(text) with
        |   Error er -> Assert.False(true,"failed to extract coords")
        |   Ok(lat,lon) ->
            Assert.Equal(55.81373210,lat, 10)
            Assert.Equal(37.81203200,lon, 10)
    }

[<Fact(Skip="Needs update to match new website structure")>]
let ``Extract cards from catalog``() =
    async {
        let! doc = loadAndParseHtmlTestFile("catalog.html.dump")
        match getCatalogCards doc with
        |   Error er -> Assert.True(false,sprintf "Failed to get cards from catalog: %s" er)
        |   Ok(cards) ->
            Assert.Equal(20, cards.Length)
            Assert.True(cards |> Seq.exists (fun x -> x.ID="rf494635" && x.url="https://pet911.ru/%D0%A2%D0%B2%D0%B5%D1%80%D1%8C/найдена/собака/rf494635"))
    }