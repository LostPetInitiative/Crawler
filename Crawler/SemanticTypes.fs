module Kashtanka.SemanticTypes

type Species = 
    |   dog = 1
    |   cat = 2
type Sex = 
|   unknown = 1
|   male = 2
|   female = 3
type CardType =
|   lost = 1
|   found = 2
type PetPhoto = {
    /// Filename with extension
    id: string
    url: string
}
type Author = {
    username: string
    phone: string
    email: string
}
type PetCard = {
    art: string
    photos : PetPhoto[]
    animal: Species
    sex: Sex
    address: string
    latitude: string
    longitude: string
    date: string // %Y-%m-%dT%H:%M:%SZ
    ``type``: CardType
    description: string
    author: Author
}
