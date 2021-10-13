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