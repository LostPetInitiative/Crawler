module Kashtanka.PhotosForCardCrawler

open Crawler

type private Msg =
    /// cardID -> Photos
    |   ProcessCard of string * Set<RemoteResourseDescriptor> * AsyncReplyChannel<Result<unit,string>>
    |   PhotoProcessed of string * Result<unit,string>
    |   ShutdownRequest of AsyncReplyChannel<unit>

type private AgentState = {
    /// cardID -> ongoing photo fetches (photo ID) * ready channel
    ongoingJobs: Map<string,Set<string> * AsyncReplyChannel<Result<unit,string>>>
    photoToCard: Map<string, string>
    shuttingDown: AsyncReplyChannel<unit> option
}

/// Maintains the list of photo retrieval jobs for each cards. Notifies when all of the photos for a card are processed
/// processPhoto: card -> process:(photoID -> result)
type Agent<'T>(processPhoto: RemoteResourseDescriptor -> Async<Result<unit,string>>) =
    let mbProcessor = MailboxProcessor<Msg>.Start(fun inbox ->
        let rec messageLoop (state:AgentState) = async {
            let! nextState = async {
                match! inbox.Receive() with
                |   ShutdownRequest channel ->                
                    return {
                        state with
                            shuttingDown = Some channel
                    }
                |   ProcessCard(cardID, photos, readyChannel) ->
                    let processPhotoAndRegisterCompletion photo =
                        async {
                            let! result = processPhoto photo
                            inbox.Post(PhotoProcessed (photo.ID,result))
                        } |> Async.Start
                    photos |> Seq.iter processPhotoAndRegisterCompletion
                    
                    if Set.isEmpty photos then
                        readyChannel.Reply(Ok())
                        return state
                    else
                        return {
                            state with
                                ongoingJobs = Map.add cardID (Set.map (fun x -> x.ID) photos, readyChannel) state.ongoingJobs
                                photoToCard = photos |> Set.fold (fun m photo -> Map.add photo.ID cardID m) state.photoToCard
                        }
                |   PhotoProcessed(photoID, result) ->
                    let cardID = Map.find photoID state.photoToCard
                    let ongoingCardJobs,readyChannel = Map.find cardID state.ongoingJobs
                    let updatedCardJobs = Set.remove photoID ongoingCardJobs
                    let ongoingJobs, photoToCard =
                        if Set.isEmpty updatedCardJobs then
                            readyChannel.Reply(result)
                            (Map.remove cardID state.ongoingJobs,
                                Map.remove photoID state.photoToCard)
                        else
                            match result with
                            |   Error e ->
                                // on first failure, we report the card result and de-register all photo tasks
                                readyChannel.Reply(result)
                                (Map.remove cardID state.ongoingJobs,
                                    Set.fold (fun m pID -> Map.remove pID m) state.photoToCard ongoingCardJobs)
                            |   Ok() ->
                                (Map.add cardID (updatedCardJobs,readyChannel) state.ongoingJobs,
                                    Map.remove photoID state.photoToCard)
                    return {
                        state with
                            ongoingJobs = ongoingJobs
                            photoToCard = photoToCard
                    }                
                }
            
            match Map.isEmpty nextState.ongoingJobs, nextState.shuttingDown with
            |   true, Some(channel) ->
                channel.Reply()
                ()
            |   _ -> return! messageLoop nextState
            }

        let initialState:AgentState = {
            ongoingJobs = Map.empty
            photoToCard = Map.empty
            shuttingDown = None
        }
        
        messageLoop initialState)

    member _.AwaitAllPhotos cardID photoDescriptors =
        mbProcessor.PostAndAsyncReply(fun x -> ProcessCard(cardID, Set.ofSeq photoDescriptors, x))

    member _.Shutdown() =
        mbProcessor.PostAndAsyncReply(fun x -> ShutdownRequest x)
        

