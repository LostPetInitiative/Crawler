module Parsers.Pet911

open System
open System.IO
open Xunit

open Kashtanka.Common
open Kashtanka.pet911.Parsers
open HtmlAgilityPack
open Kashtanka.SemanticTypes

let dataDir = "../../../../data/20240114/"

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
let ``Extract species (lost cat female)`` () =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl518787.html.dump"
        let parseRes = getAnimalSpecies doc
        Assert.Equal(Species.cat, extractSuccessful(parseRes))
    }

[<Fact>]
let ``Extract species (found dog male)`` () =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rf518209.html.dump"
        let parseRes = getAnimalSpecies doc
        Assert.Equal(Species.dog, extractSuccessful(parseRes))
    }

[<Fact>]
let ``Extract species (lost cat male)`` () =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl537378_lost_cat_male.html.dump"
        let parseRes = getAnimalSpecies doc
        Assert.Equal(Species.cat, extractSuccessful(parseRes))
    }


[<Fact>]
let ``Extract photo URLs`` () =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl518787.html.dump"
        let parseRes = getPhotoUrls doc
        Assert.Equal(7, extractSuccessful(parseRes).Length)
        Assert.Contains("https://cdn.pet911.ru/Pet_165095340062678cb83dea18.58046461.webp",extractSuccessful(parseRes))
        Assert.Contains("https://cdn.pet911.ru/Pet_165095343462678cda7583a1.69548470.webp",extractSuccessful(parseRes))
        Assert.Contains("https://cdn.pet911.ru/Pet_165095351562678d2bb44ab4.37666840.webp",extractSuccessful(parseRes))
        Assert.Contains("https://cdn.pet911.ru/Pet_165095351662678d2c27d440.01200981.webp",extractSuccessful(parseRes))
        Assert.Contains("https://cdn.pet911.ru/Pet_165095355562678d53ce94c6.03768364.webp",extractSuccessful(parseRes))
        Assert.Contains("https://cdn.pet911.ru/Pet_165095355662678d54a27803.69782174.webp",extractSuccessful(parseRes))
        Assert.Contains("https://cdn.pet911.ru/Pet_165095355762678d5570db96.27027701.webp",extractSuccessful(parseRes))
    }

[<Fact>]
let ``Extract photo ID (CDN 1)`` () =
    let parseRes = getPhotoId(Uri("https://cdn.pet911.ru/thumb_1654448834629ce2c249c577.33157738_image.webp"))
    Assert.Equal(Ok("thumb_1654448834629ce2c249c577.33157738_image.webp"),parseRes)

[<Fact>]
let ``Extract photo ID (CDN 2)`` () =
    let parseRes = getPhotoId(Uri("https://cdn.pet911.ru/thumb_Pet_165095343462678cda7583a1.69548470.webp"))
    Assert.Equal(Ok("thumb_Pet_165095343462678cda7583a1.69548470.webp"),parseRes)

[<Fact>]
let ``Extract photo ID (CDN 3)`` () =
    let parseRes = getPhotoId(Uri("https://cdn.pet911.ru/thumb_165521764662a89dee7a9f67.94353445_1.webp"))
    Assert.Equal(Ok("thumb_165521764662a89dee7a9f67.94353445_1.webp"),parseRes)

[<Fact>]
let ``Extract photo ID (upload)`` () =
    let parseRes = getPhotoId(Uri("https://pet911.ru/upload/d2/2022_06/165521425862a890b29d17a3.55330430_7BA6C9051EFD4B21A537967B2D129936.jpeg"))
    Assert.Equal(Ok("165521425862a890b29d17a3.55330430_7BA6C9051EFD4B21A537967B2D129936.jpeg"),parseRes)

[<Fact>]
let ``Card with no photos`` () =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl518678_no_photos.html.dump"
        let parseRes = getPhotoUrls doc
        Assert.Equal(0, extractSuccessful(parseRes).Length)        
    }

[<Fact>]
let ``Extract event time``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rf518209.html.dump"
        let parseRes = getEventTimeUTC(doc) 
        Assert.Equal(System.DateTime(2022,4,22),extractSuccessful(parseRes))
    }

[<Fact>]
let ``Extract author name (found card)``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rf518209.html.dump"
        let authorRes = getAuthorName(doc) 
        Assert.Equal(Some("Максим"),extractSuccessful(authorRes))
    }

[<Fact>]
let ``Extract author name (lost card)``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl527005_lost_author_name.html.dump"
        let authorRes = getAuthorName(doc) 
        Assert.Equal(Some("Дмитрий"),extractSuccessful(authorRes))
    }

[<Fact>]
let ``Extract author name for lost card that is found``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl537200_lost_is_found.html.dump"
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

[<Fact>]
let ``Extract event address``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl518787.html.dump"
        let messageRes = getEventAddress(doc) 
        Assert.Equal("улица Брянский Пост, 6 с1А, Москва", extractSuccessful(messageRes))
    }

[<Fact>]
let ``Extract animal sex``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rf518209.html.dump"
        let sexRes = getAnimalSex(doc) 
        Assert.Equal(Sex.male, extractSuccessful(sexRes))
    }

[<Fact>]
let ``Animal sex unknown``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rf494618_no_sex.html.dump"
        let sexRes = getAnimalSex(doc) 
        Assert.Equal(Sex.unknown, extractSuccessful(sexRes))
    }

[<Fact>]
let ``No author``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rf494610_no_author.html.dump"
        let authorName = getAuthorName(doc) 
        Assert.False(hasFailed(authorName))
        Assert.Equal(None,extractSuccessful(authorName))
    }

[<Fact>]
let ``Extract event type - loss``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rl518787.html.dump"
        let eventTypeRes = getEventType(doc) 
        Assert.Equal(EventType.lost, extractSuccessful(eventTypeRes))
    }

[<Fact>]
let ``Extract event type - find``() =
    async {
        let! doc = loadAndParseHtmlTestFile "petCard_rf518209.html.dump"
        let eventTypeRes = getEventType(doc) 
        Assert.Equal(EventType.found, extractSuccessful(eventTypeRes))
    }

[<Fact>]
let ``Extract event coords``() =
    async {
        let! text = IO.File.ReadAllTextAsync(Path.Combine(dataDir,"petCard_rl518787.html.dump")) |> Async.AwaitTask
        match getEventCoords(text) with
        |   Error er -> Assert.False(true,"failed to extract coords")
        |   Ok(lat,lon) ->
            Assert.Equal(55.77292439,lat, 10)
            Assert.Equal(37.55103469,lon, 10)
    }

[<Fact>]
let ``Extract cards from catalog``() =
    async {
        let! doc = loadAndParseHtmlTestFile("catalog.html.dump")
        match getCatalogCards doc with
        |   Error er -> Assert.True(false,sprintf "Failed to get cards from catalog: %s" er)
        |   Ok(cards) ->
            Assert.Equal(20, cards.Length)
            Assert.True(cards |> Seq.exists (fun x -> x.ID="rl784284" && x.url="https://pet911.ru/moskva/lost/cat/rl784284"))
    }

[<Fact>]
let ``Issue 45 temp image locations`` () =
    async {
        let! doc = loadAndParseHtmlTestFile "../20220724/rl546939.html.dump"
        match getPhotoUrls doc with
        | Error er -> Assert.True(false, sprintf "expected to extract URLS, but got %A" er)
        | Ok(urls) ->
            Assert.NotEmpty(urls)
    }
