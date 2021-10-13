module Kashtanka.Common

let hasFailed (result:Result<_,_>) =
    match result with
    |   Error _ -> true
    |   Ok _ -> false

let extractSuccessful (result:Result<'T,_>) =
    match result with
    |   Error e -> failwithf "can't extract successful result: %A" e
    |   Ok v -> v