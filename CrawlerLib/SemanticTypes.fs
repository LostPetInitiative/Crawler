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
type PetPhoto = {
    /// Filename with extension
    id: string
    url: string
}
type Author = {
    name: string
    phone: string
    email: string
}
type PetCard = {
    id: string
    photos : PetPhoto[]
    animal: Species
    sex: Sex
    address: string
    latitude: string
    longitude: string
    date: System.DateTime
    ``type``: EventType
    description: string
    author: Author
}
