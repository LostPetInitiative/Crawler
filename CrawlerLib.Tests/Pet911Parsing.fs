module Parsers.Pet911

open System
open System.IO
open Xunit

open Kashtanka.Common
open Kashtanka.Parsers
open HtmlAgilityPack
open Kashtanka.SemanticTypes

let dataDir = "../../../../data"

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
        let! doc = loadAndParseHtmlTestFile "petCard_rl476712.html"
        let parseRes = pet911.getCardId doc
        Assert.Equal("rl476712",extractSuccessful(parseRes))
    }

[<Fact>]
let ``Extract cat species`` () =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl476712.html"
        let parseRes = pet911.getAnimalSpecies doc
        Assert.Equal(Species.cat, extractSuccessful(parseRes))
    }

[<Fact>]
let ``Extract photo URLs`` () =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl476712.html"
        let parseRes = pet911.getPhotoUrls doc
        Assert.Equal(4, extractSuccessful(parseRes).Length)
        Assert.Contains("https://pet911.ru/upload/Pet_thumb_162560784360e4cea36deb30.11666472.jpeg",extractSuccessful(parseRes))
        Assert.Contains("https://pet911.ru/upload/Pet_thumb_162560784360e4cea3b97075.01962179.jpeg",extractSuccessful(parseRes))
        Assert.Contains("https://pet911.ru/upload/Pet_thumb_162560784460e4cea4185588.03311919.jpeg",extractSuccessful(parseRes))
        Assert.Contains("https://pet911.ru/upload/Pet_thumb_162560784460e4cea4998d43.72303042.jpeg",extractSuccessful(parseRes))
    }

[<Fact>]
let ``Extract event time``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl476712.html"
        let parseRes = pet911.getEventTimeUTC(doc) 
        Assert.Equal(System.DateTime(2021,6,26),extractSuccessful(parseRes))
    }

[<Fact>]
let ``Extract author name``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl476712.html"
        let authorRes = pet911.getAuthorName(doc) 
        Assert.Equal("Анастасия",extractSuccessful(authorRes))
    }

[<Fact>]
let ``Extract author message``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl476712.html"
        let messageRes = pet911.getAuthorMessage(doc) 
        Assert.Equal("Сломанных хвостик на конце в двух местах, вислоухий, крупные передние лапы, оранжевые глаза, пугливый, жмётся к земле, был в голубом ошейнике от блох", extractSuccessful(messageRes))
    }

[<Fact>]
let ``Extract event address``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl476712.html"
        let messageRes = pet911.getEventAddress(doc) 
        Assert.Equal("11 к1, Чусовская улица, район Гольяново, Москва, Центральный федеральный округ, 107207, Россия", extractSuccessful(messageRes))
    }

[<Fact>]
let ``Extract animal sex``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl476712.html"
        let sexRes = pet911.getAnimalSex(doc) 
        Assert.Equal(Sex.male, extractSuccessful(sexRes))
    }

[<Fact>]
let ``Extract event type``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl476712.html"
        let eventTypeRes = pet911.getEventType(doc) 
        Assert.Equal(EventType.lost, extractSuccessful(eventTypeRes))
    }