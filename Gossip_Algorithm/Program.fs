module Program
open System
open System.Diagnostics
open GossipModule


[<EntryPoint>]
let main argv =

    let noOfNodes = Int32.Parse(argv.[0])
    let topology = argv.[1]
    let algorithm = argv.[2]

    GossipModule.gossipSimulator (noOfNodes) (topology) (algorithm)

    0 