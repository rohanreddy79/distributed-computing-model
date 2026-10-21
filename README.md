# Gossip and Push-Sum Simulator

**An Akka.NET actor simulation, written in F#, that measures how long the gossip and push-sum protocols take to converge on line, full, 2D-grid and imperfect-3D-grid topologies.**

## Overview

Gossip-style protocols spread a rumour (gossip) or compute an aggregate (push-sum) by having every node talk only to randomly chosen neighbours, which makes them simple and fault-tolerant but makes their convergence time depend heavily on the network shape. This project spawns `N` Akka.NET actors, wires them into one of four topologies, starts a protocol at a random node and prints the wall-clock time until every node has converged. It is a single .NET 8 console application built on `Akka.FSharp` 1.5.14: each node is an actor, a supervisor actor counts convergence reports and stops a `Stopwatch`, and a helper actor keeps the rumour alive in sparse topologies. The build artefacts checked in alongside the source show it was built by the same team as the companion `Chord-distributed-hash-table-` and `concurrent-client-server` repositories (COP 5615, Distributed Operating System Principles, University of Florida, fall 2023).

## What it does

- Accepts three command-line arguments: `<numNodes> <topology> <algorithm>`.
- Topologies (`Gossip.fsx`): `line` (neighbours `i-1`, `i+1`), `full` (every other node), `2D` (a `ceil(sqrt N)` grid with 4-neighbour adjacency) and `Imp3D` (a `ceil(cbrt N)` cube with 6 grid neighbours plus one extra random neighbour per node).
- Algorithms: `gossip` (rumour spreading; a node is converged once its rumour counter has reached 10) and `push-sum` (each node starts with `s = i, w = 1`, repeatedly halves its pair and sends half to a random neighbour, and is converged once the change in `s/w` stays below `1e-10` for three consecutive rounds; the round counter is initialised to 1, so two quiet rounds suffice before any reset).
- Reports a single line, `Time for convergence: <ms> ms`, when the supervisor has received a convergence report from every node, then exits.

## How it works

| File | Role |
| --- | --- |
| `Gossip_Algorithm/Program.fs` | Entry point: parses `argv` and calls `GossipModule.gossipSimulator`. |
| `Gossip_Algorithm/Gossip.fsx` | `GossipModule`: message types, the `Supervisor`, `Worker` and `ActorWorker` actors, and the four topology builders. |
| `Gossip_Algorithm/Gossip_Algorithm.fsproj` | .NET 8 console project; the only package reference is `Akka.Fsharp` 1.5.14. |
| `Gossip_Algorithm/bin/`, `obj/` | Committed Debug build output (net8.0). |

Message types are a single discriminated union, `GossipMessageTypes`: `Initailize of IActorRef[]` (neighbour list, spelled that way in the source), `InitializeVariables of int` (push-sum starting value), `ActivateWorker`, `CallWorker`, `ReportMessageReceived`, `StartPushSum of float`, `ComputePushSum of s * w * delta`, `Result of s * w`, `TotalNodes`, `Time` and `AddNeighbors`.

Topology reference (`N` = `numNodes` from the command line):

| `topology` | Nodes spawned | Neighbours of a node |
| --- | --- | --- |
| `line` | `N + 1` | the previous and next index |
| `full` | `N + 1` | every other node |
| `2D` | `N + 1` | up to 4 grid neighbours in a `ceil(sqrt N)` square |
| `Imp3D` | `N` | up to 6 grid neighbours in a `ceil(cbrt N)` cube, plus one random node |

Key design points:

- **Worker actor (one per node).** For gossip, the first `CallWorker` starts an `ActivateWorker` self-loop; each iteration forwards `CallWorker` to one random neighbour that is not yet marked converged in a shared `Dictionary<IActorRef, bool>`. Once the counter has reached 10, the next `CallWorker` (the 11th) makes the node report to the supervisor and mark itself converged, and its forwarding loop stops. For push-sum, `ComputePushSum` adds the incoming `(s, w)`, checks the change in `s/w` against `delta`, halves the new pair and forwards half; a converged node keeps relaying incoming mass without updating its own.
- **ActorWorker (kick-starter).** A single helper actor spawned as `"ActorWorker"` holds the whole node list and keeps sending `CallWorker` to random non-converged nodes, pruning converged ones from its list. It is started for `line`, `2D` and `Imp3D` so the rumour cannot die out before every node has heard it; `full` does not need it.
- **Supervisor.** Receives `TotalNodes N`, counts `ReportMessageReceived` / `Result` messages, and when the count reaches `N` stops the `Stopwatch`, prints the elapsed milliseconds and calls `Environment.Exit 0`.
- **Timing.** The stopwatch starts after all actors are spawned and wired, immediately before the first protocol message is sent, so the reported time excludes actor creation.

## Running it

Requires the .NET 8 SDK. Not verified on this machine (`dotnet` is not installed here).

```sh
cd Gossip_Algorithm
dotnet run -- <numNodes> <topology> <algorithm>
#   topology:  line | full | 2D | Imp3D
#   algorithm: gossip | push-sum

dotnet run -- 100 full gossip
dotnet run -- 64 2D push-sum
dotnet run -- 1000 Imp3D gossip
```

The program prints `Starting ...` / `------------- Begin ... -------------`, then `Time for convergence: <ms> ms`, and exits.

## Limitations / Notes

- `2D` indexes the grid as `i * gridSize + j` over an array of `N + 1` actors, so it only works when `N` is a perfect square (16, 64, 100, ...); other values raise an index-out-of-range error. The `"Imp2D"` string appears in the node-count rounding but is not an implemented topology.
- `line`, `full` and `2D` spawn `N + 1` actors (an inclusive `[0..nodes]` range) while convergence waits for `N` reports, so one extra actor is always present; `Imp3D` spawns exactly `N`.
- A single wall-clock number is printed per run; there is no repetition, averaging or CSV output, so comparisons across topologies need several manual runs.
- The `#r "nuget: ..."` lines at the top of `Gossip.fsx` are leftovers from running the file as a script; the `.fsproj` package reference is what actually supplies Akka when building with `dotnet`.
- The `bin/` and `obj/` folders were committed with the source; a `.gitignore` has been added so future builds do not add more.
