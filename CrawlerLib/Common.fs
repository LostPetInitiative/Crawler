module Kashtanka.Common

let hasFailed (result:Result<_,_>) =
    match result with
    |   Error _ -> true
    |   Ok _ -> false

let extractSuccessful (result:Result<'T,_>) =
    match result with
    |   Error e -> failwithf "can't extract successful result: %A" e
    |   Ok v -> v

type IAsyncSet<'TKey> =
    interface
        abstract member Exists: 'TKey -> Async<bool>
        abstract member Add: 'TKey -> Async<unit>
    end

let allResults (results:Result<'Success,'Error> seq) =
    let rec build (results:Result<'Success,'Error> list) built = 
        match results with
        |   [] -> Ok(built |> List.rev)
        |   head::tail ->
            match head with
            |   Ok(success) -> build tail (success::built)
            |   Error(error) -> Error(error)
    build (List.ofSeq results) []

let (|InterpretedMatch|_|) pattern input =
    if input = null then None
    else
        let m = System.Text.RegularExpressions.Regex.Match(input, pattern)
        if m.Success then Some [for x in m.Groups -> x]
        else None
