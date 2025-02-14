module GossipModule
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Diagnostics
open System.Collections.Generic

type GossipMessageTypes =
    | Initailize of IActorRef []
    | InitializeVariables of int
    | ReportMessageReceived of String
    | StartPushSum of Double
    | ComputePushSum of Double * Double * Double
    | Result of Double * Double
    | Time of int
    | TotalNodes of int
    | ActivateWorker 
    | CallWorker
    | AddNeighbors

type Topology = 
    | Gossip of String
    | PushSum of String

type Protocol = 
    | Line of String
    | Full of String
    | TwoDimension of String


let gossipSimulator noOfNodes topology algorithm=
    let mutable nodes = noOfNodes
    let topology = topology
    let protocol = algorithm
    let timer = Diagnostics.Stopwatch()
    let system = ActorSystem.Create("System")
    let mutable actualNoOfNodes = nodes |> float
    nodes <-
        match topology with
        | "2D" | "Imp2D" -> 
            ((actualNoOfNodes * 0.5)|>ceil ) * 2.0 |> int
        | _ -> nodes

    let mutable  nodeArray = [||]

    let Supervisor(mailbox: Actor<_>) =
        
        let mutable count = 0
        let mutable start = 0
        let mutable totalNodes = 0

        let rec loop()= actor{
            let! msg = mailbox.Receive();
            match msg with 
            | ReportMessageReceived _ -> 
                let ending = DateTime.Now.TimeOfDay.Milliseconds
                count <- count + 1
                if count = totalNodes then
                    timer.Stop()
                    printfn "Time for convergence: %f ms" timer.Elapsed.TotalMilliseconds
                    Environment.Exit(0)
            | Result (sum, weight) ->
                count <- count + 1
                if count = totalNodes then
                    timer.Stop()
                    
                    printfn "Time for convergence: %f ms" timer.Elapsed.TotalMilliseconds
                    Environment.Exit(0)
            | Time strtTime -> start <- strtTime
            | TotalNodes n -> totalNodes <- n
            | _ -> ()

            return! loop()
        }            
        loop()
    ////////////////////////////Supervisor Actor////////////////////////////////////////

    let supervisor = spawn system "Supervisor" Supervisor
    let dictionary = new Dictionary<IActorRef, bool>()

    ///////////////////////////Worker Actor////////////////////////////////////////
    let Worker(mailbox: Actor<_>) =
        let mutable rumourCount = 0
        let mutable neighbours: IActorRef [] = [||]
        let mutable sum = 0 |>double
        let mutable weight = 1.0
        let mutable termRound = 1
        let mutable alreadyConverged = false
        
        
        let rec loop()= actor{
            let! message = mailbox.Receive();
            
            match message with 

            | Initailize aref ->
                neighbours <- aref

            | ActivateWorker ->
                if rumourCount < 11 then
                    let rnd = Random().Next(0, neighbours.Length)
                    if not dictionary.[neighbours.[rnd]] then
                        neighbours.[rnd] <! CallWorker
                    mailbox.Self <! ActivateWorker

            | CallWorker ->
                
                if rumourCount = 0 then 
                    mailbox.Self <! ActivateWorker
                if (rumourCount = 10) then 
                    supervisor <! ReportMessageReceived "Rumor"
                    dictionary.[mailbox.Self] <- true
                rumourCount <- rumourCount + 1
                
            | InitializeVariables number ->
                sum <- number |> double

            | StartPushSum delta ->
                let index = Random().Next(0, neighbours.Length)

                sum <- sum / 2.0
                weight <- weight / 2.0
                neighbours.[index] <! ComputePushSum(sum, weight, delta)

            | ComputePushSum (s: float, w, delta) ->
                let newsum = sum + s
                let newweight = weight + w

                let cal = sum / weight - newsum / newweight |> abs

                if alreadyConverged then

                    let index = Random().Next(0, neighbours.Length)
                    neighbours.[index] <! ComputePushSum(s, w, delta)
                
                else
                    if cal > delta then
                        termRound <- 0
                    else 
                        termRound <- termRound + 1

                    if  termRound = 3 then
                        termRound <- 0
                        alreadyConverged <- true
                        supervisor <! Result(sum, weight)
                
                    sum <- newsum / 2.0
                    weight <- newweight / 2.0
                    let index = Random().Next(0, neighbours.Length)
                    neighbours.[index] <! ComputePushSum(sum, weight, delta)
            | _ -> ()
            return! loop()
        }            
        loop()



    let ActorWorker (mailbox: Actor<_>) =
        let neighbors = new List<IActorRef>()
        let rec loop() = actor {
            let! message = mailbox.Receive()
            match message with 
            | AddNeighbors ->
                for i in [0..nodes-1] do
                        neighbors.Add nodeArray.[i]
                mailbox.Self <! ActivateWorker
            | ActivateWorker ->
                if neighbors.Count > 0 then
                    let randomNumber = Random().Next(neighbors.Count)
                    let randomActor = neighbors.[randomNumber]
                    
                    if (dictionary.[neighbors.[randomNumber]]) then  
                        (neighbors.Remove randomActor) |>ignore
                    else 
                        randomActor <! CallWorker
                    mailbox.Self <! ActivateWorker 
            | _ -> ()
            return! loop()
        }
        loop()


    let GossipActor = spawn system "ActorWorker" ActorWorker

    ///////////////////////////Worker Actor////////////////////////////////////////

    ///////////////////////////Program////////////////////////////////////////
    match topology with
    | "line" ->
        nodeArray <- Array.zeroCreate (nodes + 1)
        
        for x in [0..nodes] do
            let key: string = "demo" + string(x) 
            let actorRef = spawn system (key) Worker
            nodeArray.[x] <- actorRef 
            dictionary.Add(nodeArray.[x], false)
            nodeArray.[x] <! InitializeVariables x

        for i in [ 0 .. nodes ] do
            let mutable neighbourArray = [||]
            if i = 0 then
                neighbourArray <- (Array.append neighbourArray [|nodeArray.[i+1]|])
            elif i = nodes then
                neighbourArray <- (Array.append neighbourArray [|nodeArray.[i-1]|])
            else 
                neighbourArray <- (Array.append neighbourArray [| nodeArray.[(i - 1)] ; nodeArray.[(i + 1 ) ] |] ) 
            
            nodeArray.[i] <! Initailize(neighbourArray)

        let leader = Random().Next(0, nodes)
    
        timer.Start()
        match protocol with
        | "gossip" -> 
            supervisor <! TotalNodes(nodes)
            supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
            printfn "Starting Protocol Gossip"
            nodeArray.[leader] <! ActivateWorker
            GossipActor<! AddNeighbors
            
        | "push-sum" -> 
            supervisor <! TotalNodes(nodes)
            supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
            printfn "Starting Push Sum Protocol for Line"
            nodeArray.[leader] <! StartPushSum(10.0 ** -10.0)     
        | _ ->
            printfn "Invalid topology"   

    | "full" ->
        nodeArray <- Array.zeroCreate (nodes + 1)
        
        for x in [0..nodes] do
            let key: string = "demo" + string(x) 
            let actorRef = spawn system (key) Worker
            nodeArray.[x] <- actorRef 
            nodeArray.[x] <! InitializeVariables x
            dictionary.Add(nodeArray.[x], false)

        
        for i in [ 0 .. nodes ] do
            let mutable neighbourArray = [||]
            for j in [0..nodes] do 
                if i <> j then
                    neighbourArray <- (Array.append neighbourArray [|nodeArray.[j]|])
            nodeArray.[i]<! Initailize(neighbourArray)
                

        timer.Start()
        //random worker to start the gossip
        let leader = Random().Next(0, nodes)

        match protocol with
        | "gossip" -> 
            supervisor <! TotalNodes(nodes)
            supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
            printfn "------------- Begin Gossip -------------"
            nodeArray.[leader] <! CallWorker
        | "push-sum" -> 
            supervisor <! TotalNodes(nodes)
            supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
            printfn "------------- Begin Push Sum -------------"
            nodeArray.[leader] <! StartPushSum(10.0 ** -10.0) 
        | _ ->
            printfn "Invalid topology"

    | "2D" ->
        let gridSize = nodes |> float |> sqrt |> ceil |> int 
        nodeArray <- Array.zeroCreate (nodes+1)
        
        for x in [0..nodes] do
            let key: string = "demo" + string(x) 
            let actorRef = spawn system (key) Worker
            
            nodeArray.[x] <- actorRef 
            nodeArray.[x] <! InitializeVariables x
            dictionary.Add(nodeArray.[x], false)
            
        for i in [ 0 .. (gridSize-1)] do
            for j in [ 0 .. (gridSize-1) ] do
                let mutable neighbours: IActorRef [] = [||]
                if j + 1 < gridSize then
                    neighbours <- (Array.append neighbours [| nodeArray.[i * gridSize + j + 1] |])
                if  j - 1 >= 0 then 
                    neighbours <- (Array.append neighbours [| nodeArray.[i * gridSize + j - 1] |])
                if i - 1 >= 0 then
                    neighbours <- Array.append neighbours [| nodeArray.[(i - 1 ) * gridSize + j ] |]
                if  i + 1 < gridSize then
                    neighbours <- (Array.append neighbours [| nodeArray.[(i + 1) * gridSize + j] |])
                nodeArray.[i * gridSize + j] <! Initailize(neighbours)
        
        //random worker to start the gossip
        let leader = Random().Next(0, nodes)
        timer.Start()

        match protocol with 
        | "gossip" -> 
            supervisor <! TotalNodes(nodes)
            supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
            printfn "------------- Begin Gossip -------------"
            nodeArray.[leader] <! ActivateWorker
            GossipActor<! AddNeighbors
        | "push-sum" -> 
            supervisor <! TotalNodes(nodes)
            supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
            printfn "------------- Begin Push Sum -------------"
            nodeArray.[leader] <! StartPushSum(10.0 ** -10.0)
        | _ -> 
            printfn "Invalid topology"

    | "Imp3D" ->
        let cubeSize = nodes |> float |> (fun n -> n ** (1.0 / 3.0)) |> ceil |> int 



        nodeArray <- Array.zeroCreate (pown cubeSize 3)

        for x in [0..(nodes-1)] do
            let key: string = "demo" + string(x) 
            let actorRef = spawn system (key) Worker
            nodeArray.[x] <- actorRef 
            nodeArray.[x] <! InitializeVariables x
            dictionary.[actorRef] <- false

        for i in [0..(cubeSize-1)] do
            for j in [0..(cubeSize-1)] do
                for k in [0..(cubeSize-1)] do
                    let mutable neighbours: IActorRef [] = [||]
                    let index = i * cubeSize * cubeSize + j * cubeSize + k
                    if index < nodeArray.Length && nodeArray.[index] <> null then
                        // Neighbors in 3D grid
                        let addNeighbour ni nj nk =
                            let nIndex = ni * cubeSize * cubeSize + nj * cubeSize + nk
                            if nIndex < nodeArray.Length && nodeArray.[nIndex] <> null then
                                neighbours <- Array.append neighbours [| nodeArray.[nIndex] |]

                        if i + 1 < cubeSize then addNeighbour (i + 1) j k
                        if i - 1 >= 0 then addNeighbour (i - 1) j k
                        if j + 1 < cubeSize then addNeighbour i (j + 1) k
                        if j - 1 >= 0 then addNeighbour i (j - 1) k
                        if k + 1 < cubeSize then addNeighbour i j (k + 1)
                        if k - 1 >= 0 then addNeighbour i j (k - 1)
                        
                        let rnd = Random().Next(0, nodes-1)
                        if nodeArray.[rnd] <> null then
                            neighbours <- Array.append neighbours [| nodeArray.[rnd] |]

                        nodeArray.[index] <! Initailize(neighbours)
                
        let leader = Random().Next(0, nodes)
        timer.Start()
        match protocol with 
        | "gossip" -> 
            supervisor <! TotalNodes(nodes)
            supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
            printfn "------------- Begin Gossip -------------"
            nodeArray.[leader] <! ActivateWorker
            GossipActor<! AddNeighbors
        | "push-sum" -> 
            supervisor <! TotalNodes(nodes)
            supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
            printfn "------------- Begin Push Sum -------------"
            nodeArray.[leader] <! StartPushSum(10.0 ** -10.0)
        | _ -> 
            printfn "Invalid topology"
    | _ -> ()
    Console.ReadLine() |> ignore
    