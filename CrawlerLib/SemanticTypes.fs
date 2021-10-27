module Kashtanka.SemanticTypes

type Species = 
    |   dog = 1
    |   cat = 2
type Sex = 
    |   unknown = 1
    |   male = 2
    |   female = 3
type EventType =
    |   lost = 1
    |   found = 2

type Author = {
    name: string
    phone: string option
    email: string option
}
type PetCard = {
    id: string
    photos : Crawler.RemoteResourseDescriptor[]
    animal: Species
    sex: Sex
    address: string
    latitude: float option
    longitude: float option
    date: System.DateTime
    ``type``: EventType
    description: string
    author: Author
}
