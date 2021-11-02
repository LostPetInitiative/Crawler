module Kashtanka.MissingResourceTracker

open System.IO
open System.Threading
open System.Collections.Generic

type MissingResourceTracker = {
    Check: string -> Async<Result<bool,string>>
    Add: string -> Async<Result<unit,string>>
}

let createFileBackedMissingResourceTracker filename : Async<MissingResourceTracker> = 
    async {
        let! initialEntries = 
            async {
                if File.Exists filename then
                    return! File.ReadAllLinesAsync(filename) |> Async.AwaitTask
                else
                    return Array.empty
            }
        let appendSem = new SemaphoreSlim(1)
        let mutable m = HashSet<string>(initialEntries |> Seq.map (fun x -> x.Trim()))

        let check identifier = async { return m.Contains(identifier) |> Ok }
        let add identifier =
            async {
                do! appendSem.WaitAsync() |> Async.AwaitTask
                try
                    try
                        let added = m.Add(identifier)
                        if added then
                            do! File.AppendAllLinesAsync(filename, Seq.singleton identifier) |> Async.AwaitTask
                        return Ok()
                    with
                    |   e -> return Error(e.ToString())
                finally
                    appendSem.Release() |> ignore

            }
        return {
            Check = check
            Add = add
        }
    }