module Kashtanka.pet911.Utils

open Kashtanka.Crawler

let cardIDsFromRange firstCard lastCard =
    seq {
        for i in firstCard..lastCard do
            yield sprintf "rf%d" i;
            yield sprintf "rl%d" i;
        }

let cardIDToURL (cardID:string) =
    System.String.Format("""https://pet911.ru/%D0%A5%D0%B0%D0%B1%D0%B0%D1%80%D0%BE%D0%B2%D1%81%D0%BA/%D0%BD%D0%B0%D0%B9%D0%B4%D0%B5%D0%BD%D0%B0/%D1%81%D0%BE%D0%B1%D0%B0%D0%BA%D0%B0/{0}""",cardID)

let cardIDtoDescriptor cardID : RemoteResourseDescriptor = 
    {
        ID = cardID;
        url = cardIDToURL cardID
    }