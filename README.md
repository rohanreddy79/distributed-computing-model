# Gossip and Push-Sum Simulator

[![CI](https://github.com/rohanreddy79/distributed-computing-model/actions/workflows/ci.yml/badge.svg)](https://github.com/rohanreddy79/distributed-computing-model/actions/workflows/ci.yml)

**An Akka.NET actor simulation, written in F#, that measures how long the gossip and push-sum protocols take to converge on line, full, 2D-grid and imperfect-3D-grid topologies.**

## Overview

Gossip-style protocols spread a rumour (gossip) or compute an aggregate (push-sum) by having every node talk only to randomly chosen neighbours, which makes them simple and fault-tolerant but makes their convergence time depend heavily on the network shape. This project spawns `N` Akka.NET actors, wires them into one of four topologies, starts a protocol at a random node and prints the wall-clock time until every node has converged. It is a single .NET 8 console application built on `Akka.FSharp` 1.5.14: each node is an actor, a supervisor actor counts convergence reports and stops a `Stopwatch`, and a helper actor keeps the rumour alive until every node has heard it. The build artefacts originally committed with the source (untracked in 2026-10, still in the git history) show it was built by the same team as the companion `Chord-distributed-hash-table-` and `concurrent-client-server` repositories (COP 5615, Distributed Operating System Principles, University of Florida, fall 2023).

## What it does

- Accepts three command-line arguments: `<numNodes> <topology> <algorithm>`.
- Topologies (`Gossip.fsx`): `line` (neighbours `i-1`, `i+1`), `full` (every other node), `2D` (the first `N` cells of a `ceil(sqrt N)` grid with 4-neighbour adjacency) and `Imp3D` (a `ceil(cbrt N)` cube with 6 grid neighbours plus one extra random neighbour per node).
- Algorithms: `gossip` (rumour spreading; a node is converged once its rumour counter has reached 10) and `push-sum` (each node starts with `s = i, w = 1`, repeatedly halves its pair and sends half to a random neighbour, and is converged once the change in `s/w` stays below `1e-10` for three consecutive rounds; the round counter is initialised to 1, so two quiet rounds suffice before any reset).
- Reports a single line, `Time for convergence: <ms> ms`, when the supervisor has received a convergence report from every node, then exits.

## How it works

| File | Role |
| --- | --- |
| `Gossip_Algorithm/Program.fs` | Entry point: parses `argv` and calls `GossipModule.gossipSimulator`. |
| `Gossip_Algorithm/Gossip.fsx` | `GossipModule`: message types, the `Supervisor`, `Worker` and `ActorWorker` actors, and the four topology builders. Compiled into the executable by the `.fsproj`, not run as a script. |
| `Gossip_Algorithm/Gossip_Algorithm.fsproj` | .NET 8 console project; the only package reference is `Akka.Fsharp` 1.5.14. |
| `.github/workflows/ci.yml` | GitHub Actions: `dotnet build` and a `dotnet run` of all eight topology × protocol combinations on `ubuntu-latest`. |

Message types are a single discriminated union, `GossipMessageTypes`: `Initailize of IActorRef[]` (neighbour list, spelled that way in the source), `InitializeVariables of int` (push-sum starting value), `ActivateWorker`, `CallWorker`, `ReportMessageReceived`, `StartPushSum of float`, `ComputePushSum of s * w * delta`, `Result of s * w`, `TotalNodes`, `Time` and `AddNeighbors`.

Topology reference (`N` = `numNodes` from the command line):

| `topology` | Nodes spawned | Neighbours of a node |
| --- | --- | --- |
| `line` | `N` | the previous and next index |
| `full` | `N` | every other node |
| `2D` | `N` rounded up to an even number | up to 4 grid neighbours in a `ceil(sqrt N)` square; only the first `N` cells hold a node |
| `Imp3D` | `N` | up to 6 grid neighbours in a `ceil(cbrt N)` cube, plus one random node |

Key design points:

- **Worker actor (one per node).** For gossip, the first `CallWorker` starts an `ActivateWorker` self-loop; each iteration forwards `CallWorker` to one random neighbour that is not yet marked converged in a shared `Dictionary<IActorRef, bool>`. Once the counter has reached 10, the next `CallWorker` (the 11th) makes the node report to the supervisor and mark itself converged, and its forwarding loop stops. For push-sum, `ComputePushSum` adds the incoming `(s, w)`, checks the change in `s/w` against `delta`, halves the new pair and forwards half; a converged node keeps relaying incoming mass without updating its own.
- **ActorWorker (kick-starter).** A single helper actor spawned as `"ActorWorker"` holds the whole node list and keeps sending `CallWorker` to random non-converged nodes, pruning converged ones from its list. It is started for every gossip run (including `full` since 2026-10): converged nodes stop forwarding, so without it the last one or two nodes can starve and the run never finishes.
- **Supervisor.** Receives `TotalNodes N`, counts `ReportMessageReceived` / `Result` messages, and when the count reaches `N` stops the `Stopwatch`, prints the elapsed milliseconds and calls `Environment.Exit 0`.
- **Timing.** The stopwatch starts after all actors are spawned and wired, immediately before the first protocol message is sent, so the reported time excludes actor creation.
- **Lifetime.** After wiring, the main thread blocks on `system.WhenTerminated` until the supervisor calls `Environment.Exit`, so the program behaves the same with or without a terminal on stdin (it used to `Console.ReadLine()`, which returns at once under CI or a pipe).

## Running it

Requires the .NET 8 SDK. Verified on macOS arm64 with .NET SDK 8.0.426:

```sh
cd Gossip_Algorithm
dotnet run -- <numNodes> <topology> <algorithm>
#   topology:  line | full | 2D | Imp3D
#   algorithm: gossip | push-sum
```

```
$ dotnet run -- 100 full gossip
------------- Begin Gossip -------------
Time for convergence: 17.818200 ms

$ dotnet run -- 11 2D push-sum
------------- Begin Push Sum -------------
Time for convergence: 18.403600 ms

$ dotnet run -- 30 Imp3D gossip
------------- Begin Gossip -------------
Time for convergence: 16.459400 ms
```

All eight topology × protocol combinations were run twice each with `N` between 11 and 100 (`100 full`, `20 line`, `11 2D`, `30 Imp3D`); every run converged in 15–53 ms and exited with status 0. CI runs the same eight combinations under a 60 s timeout each.

## Limitations / Notes

- At these sizes the printed time is dominated by message scheduling, not by the protocol (all runs land near 15–50 ms); differences between topologies only show at thousands of nodes.
- `2D` rounds `N` up to an even number before building the grid (`11` → 12 nodes); the `"Imp2D"` string appears only in that rounding and is not an implemented topology.
- A single wall-clock number is printed per run; there is no repetition, averaging or CSV output, so comparisons across topologies need several manual runs.
- `Imp3D` draws its extra random neighbour from `Random().Next(0, nodes-1)`, so the last node is never chosen as anyone's random neighbour (harmless; left as is).
- `argv` is indexed directly, so running with fewer than three arguments throws instead of printing usage.

## Changelog

- 2026-10: `line`, `full` and `2D` spawn exactly `N` actors (an inclusive `[0..nodes]` range spawned `N + 1`); `2D` works for any `N` — only the first `N` cells of the grid are wired (`11 2D` used to crash with `IndexOutOfRangeException`); `full gossip` now starts the `ActorWorker` kick-starter like the other topologies (with exactly `N` nodes the last node could starve); the main thread waits on `system.WhenTerminated` instead of `Console.ReadLine()` (the program exited immediately when stdin was not a terminal); removed the leftover `#r "nuget: ..."` lines from `Gossip.fsx`, which the compiler honoured and which overrode the pinned Akka.FSharp 1.5.14 with the latest release; untracked `bin/` and `obj/`; added MIT license and CI.
