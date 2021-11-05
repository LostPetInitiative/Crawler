module Crawlers.PhotosForCard

open Xunit

open Kashtanka
open Kashtanka.Crawler

[<Fact>]
let ``photo fetch callbacks called`` () =
    async {
        let mutable processedPhotos: List<string> = List.empty

        let lockObj = obj()

        let processPhoto (descriptor:RemoteResourseDescriptor) =
            lock lockObj (fun () ->
                processedPhotos <- descriptor.ID::processedPhotos)
            async {return Ok()}

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
        let processPhoto (descriptor:RemoteResourseDescriptor) = 
            async {if descriptor.ID = "2" then return Error("predefined failure") else return Ok()}

        let agent = PhotosForCardCrawler.Agent(processPhoto)
        match! agent.AwaitAllPhotos "card1" [ {ID="1";url="url1"}; {ID="2";url="url2"} ] with
        |   Error er ->
            Assert.Equal("predefined failure", er)
        |   Ok _ ->
            Assert.True(false)
        
        do! agent.Shutdown()

    }