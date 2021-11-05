module Crawlers.PhotosForCard

open System
open System.IO
open Xunit

open Kashtanka
open Kashtanka.Common
open Kashtanka.Crawler
open Kashtanka.SemanticTypes

open Kashtanka.CrawlerPet911
open System.Threading


[<Fact>]
let ``photo fetch callbacks called`` () =
    async {
        let mutable processedPhotos: List<string> = List.empty

        let processPhoto (descriptor:RemoteResourseDescriptor) processedCallback = 
            processedPhotos <-  descriptor.ID::processedPhotos
            processedCallback descriptor.ID (Ok())

        let agent = PhotosForCardCrawler.Agent(processPhoto)
        match! agent.AwaitAllPhotos "card1" [ {ID="1";url="url1"}; {ID="2";url="url2"} ] with
        |   Error er ->
            Assert.True(false)
        |   Ok _ ->
            Assert.Equal(2, List.length processedPhotos)
            Assert.Equal<Set<string>>(Set.ofList ["1";"2"],Set.ofList processedPhotos)
        
        do! agent.Shutdown()

    }

[<Fact>]
let ``First failure reported`` () =
    async {
        let processPhoto (descriptor:RemoteResourseDescriptor) processedCallback = 
            processedCallback descriptor.ID (if descriptor.ID = "2" then Error("predefined failure") else Ok())

        let agent = PhotosForCardCrawler.Agent(processPhoto)
        match! agent.AwaitAllPhotos "card1" [ {ID="1";url="url1"}; {ID="2";url="url2"} ] with
        |   Error er ->
            Assert.Equal("predefined failure", er)
        |   Ok _ ->
            Assert.True(false)
        
        do! agent.Shutdown()

    }