module Tests

open System
open Xunit

[<Fact>]
let ``Extract message id`` () =
    async {
        let! text = IO.File.ReadAllTextAsync("../../petCard.html") |> Async.AwaitTask
        return ()
    }
