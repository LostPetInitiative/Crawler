module Kashtanka.RangeDetection.pet911

let cardIDsFromRange firstCard lastCard =
    seq {
        for i in firstCard..lastCard do
            yield sprintf "rf%d" i;
            yield sprintf "rl%d" i;
        }

