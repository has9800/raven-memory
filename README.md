![raven logo](./raven.png) 

# Raven

**Your AI agent finally remembers.**

Most AI agents forget everything the moment you close the window. Raven fixes that. It gives any AI agent a persistent, structured memory that survives across sessions, days, and weeks, so the next time you sit down to work, your agent already knows what you were doing, what decisions were made, and what comes next.

---

## The problem with AI memory today

Every AI assistant you've used has the same fundamental flaw, it only knows what's in the current conversation. Now if you close the chat or start a new one it's a blank slate. You spend the first five minutes explaining your project, your context, your preferences.

Some tools try to fix this with notes files or conversation logs. But these are flat, just a pile of text with no understanding of cause and effect (causal relationships), no sense of what led to what, no way to go back and undo a decision.

Raven takes a different approach.

---

## How Raven works

Raven records everything that happens as a **chain of events**, where each event knows what caused it, and you can walk the chain backwards to see what went wrong. You can also rollback, and in a future release, we're going to add RGATs to build causal relationships across nodes that could derive richer explanations between relationships.

Think of it like a git commit history — but for everything your agent does and everything that happens in your project. Every action, every decision, every tool call, every note becomes a node in the chain. Each node points back to what came before it.

```
[started project]
      ↓
[ran aerodynamics sim]
      ↓
[decided to switch to titanium housing]
      ↓
[turned off lab lights] ←──┐  (ran in parallel)
[wrote CNC booking note] ←─┘
      ↓
[merged: both tasks done]
      ↓
[session ended]
```

When you come back tomorrow, Raven loads the end of that chain and hands it to your agent. The agent reads it and is instantly oriented — no re-explaining, no catching up, no lost context.

---

## What makes it a DAG, not just a list

A simple list of events would be fine for linear work. But real projects don't happen linearly. You run a simulation *while* turning off the lights. You explore two approaches in parallel before committing to one. You sometimes realize a decision was wrong and need to go back.

Raven uses a **Directed Acyclic Graph** (DAG) — a structure that can represent all of this cleanly:

**Branching** — when two things happen in parallel, the chain forks into two branches that run side by side.

**Merging** — when both parallel tasks finish, the branches automatically merge back into one chain. The agent sees a single coherent history.

**Rollback** — made a wrong decision? Roll the chain back to any earlier point. Nothing is deleted — the history is preserved, but the agent's current view moves back to a known-good state.

**Speculative nodes** — coming soon: the agent can plan ahead by adding *speculative* nodes representing what it thinks will happen next. As reality unfolds, nodes get confirmed or pruned. The agent maintains a live hypothesis about the future.

---

## Persistence that actually works

Raven stores everything in a local SQLite database — a single file on your machine. No cloud, no subscription, no data leaving your computer.

When you start a new session:

1. Raven loads the last N events from the chain
2. Your agent reads them as plain text context
3. The agent is oriented and ready — in seconds

When the session ends:

1. Every action that happened gets recorded as a new chain node
2. The database file updates
3. Next session, it's all there

This works across days, weeks, months. The chain keeps growing. Old nodes get archived to compressed storage when they're no longer needed for active context — but they're never lost. You can always go back.

---

## What the agent actually sees

At the start of every session, your agent receives something like this:

```
Last active: 3 days ago

Recent events:
  [user] decided to switch from carbon fiber to titanium housing (3 days ago)
    reason: carbon fiber too brittle under load testing
  [tool] run_simulation called (3 days ago)
    sim: repulsor_geometry_v4, result: drag coefficient 0.21
  [user] session ended (3 days ago)

Open threads: boot thrusters next, CNC time needs booking
Relevant files: /workspace/results/repulsor_v4/
Notes: titanium decision approved, moving forward
```

The agent reads this and immediately knows where the project is. No prompting, no re-explaining. It just continues.

---

## Features

**Persistent memory across sessions**
Your agent remembers what you were working on, what decisions were made, and what comes next — even weeks later.

**Causal chain structure**
Every event knows what caused it. You can trace any decision back to its origin and understand exactly how the project got to where it is.

**Parallel work with automatic merging**
Run multiple tasks at the same time. Raven tracks both, and automatically merges them back into a single coherent timeline when they're done.

**Multi-agent support**
Spawn multiple specialized subagents — a research agent, a lab agent, a home agent — each working on its own branch simultaneously. Raven merges their results back into the main chain automatically. All agents share the same memory, same chain, same context.

**Rollback to any point**
Changed your mind? Roll the chain back to any previous state. The agent picks up from there as if nothing went wrong. History is preserved — nothing is lost.

**Session-based physical state**
Raven doesn't try to track where every physical object is in real time. Instead, it records what the agent knows at the moment — and re-observes the physical world at the start of each session. Simple, honest, robust.

**Local first, privacy first**
Everything lives in a single SQLite file on your machine. No cloud, no accounts, no data leaving your computer. Run it on a Raspberry Pi, a Mac Mini, a server — anything.

**Model agnostic**
Raven works with any language model. Swap between models, upgrade to a newer one, run locally or via API — the memory layer doesn't change. Verified with Qwen3.5-4B running locally via Ollama.

**Cold storage for old history**
Configure how long events stay in active memory. Older nodes get compressed and archived automatically. A year of daily activity fits in a few megabytes.

**Concurrency safe**
Multiple agents writing simultaneously are handled correctly — writes are serialized, no data corruption, no lost nodes. Verified under concurrent load with 3 agents writing 15 nodes simultaneously.

**Speculative planning** *(coming soon)*
The agent can plan ahead by projecting future events onto the chain. As reality unfolds, speculative nodes get confirmed or pruned. The agent maintains a live hypothesis about what comes next.

**Graph-based search** *(coming soon)*
Ask questions about your history: "why did we decide X", "what was happening on March 2nd", "find all decisions related to the propulsion system." Raven searches the graph and returns relevant nodes, not just keyword matches.

---

## Example: picking up after a three-week break

You come back to a project you haven't touched in three weeks.

**Without Raven:**
You open a new chat. The agent knows nothing. You spend ten minutes explaining what you're building, what you tried, what didn't work, what comes next. You probably miss something important. The agent makes a suggestion that contradicts a decision you already made — because it doesn't know you made it.

**With Raven:**
```
Agent: Welcome back — it's been 3 weeks since we worked on Iron Man v2.

Last session you finished the right repulsor housing and approved the
titanium switch after the stress sim passed. Boot thrusters are next.

You had flagged that CNC time needs to be booked before we can continue.
Is that sorted?

You: Yeah, booked for tomorrow.

Agent: Perfect. Boot thruster sequence is next. You'll need the plasma
cutter and titanium stock. Want me to pull up the repulsor geometry
results as a reference?
```

The agent knew all of that from the chain. You were working again in 30 seconds.

---

## What's coming

Raven is actively being built. Here's what's on the roadmap:

**SQLite-Vec**
Using embeddings with SQL tables to avoid using a separate database in the future. This keeps the project lean, and footprint minimal. Once we introduce semantic search, the impact is significat. If every node has an embedding, the chain stops being just a log and becomes a searchable knowledge base about everything you've ever worked on. The longer you use Raven, the smarter it gets at finding relevant context.

**Speculative planning**
The agent will be able to project future events onto the chain — "I think we'll need to book CNC time, then run the assembly, then do final testing." These show up as *speculative* nodes. As reality unfolds, they get confirmed or pruned. The agent maintains a live hypothesis about what comes next, not just a record of what happened.

**Agentic commerce — shopping, ordering, payments**
Tell Raven to order pizza, buy parts, or book a service. It will do it through pre-approved spending limits — a virtual card with hard caps you control, connected via Marqeta or Visa's agent payment infrastructure. Your real bank account is never exposed. Every transaction is recorded in the chain. You top up a credit balance, set spending rules once, and the agent operates within them. Always with confirmation for anything above your threshold.

**Voice interface**
Talk to Raven hands-free. A local wake word detector listens passively, Whisper converts your speech to text, and the agent responds through your speakers. Designed for situations where your hands are busy — working in a lab, under a car, cooking.

**Email, web, and app integration**
Through MCP (the open standard for connecting AI to external tools), Raven will be able to read and send email, browse the web, search for information, and interact with apps — all from natural conversation. Ask it to find the torque specs for a part, draft a reply to a supplier, or check if an order shipped.

**Visual workspace awareness**
Point a camera at your workspace. At the start of each session, Raven's vision layer observes what's on the bench, what tools are out, what's partially assembled — and adds that to the chain context automatically. The agent orients itself from both memory and sight. Powered by Qwen3.5-4B's native vision capabilities — no separate model needed.

**ROS2 bridge for robotics and home automation**
Any ROS2-compatible hardware — robot arms, smart home devices, lab equipment — becomes a tool Raven can call. Same interface as everything else: the agent decides what to do, Raven calls the tool, the result gets recorded in the chain. Existing ROS2 MCP servers are already available — no custom bridge needed.

**Semantic search over history**
Ask questions about your own history: "why did we decide to switch materials?", "what were we doing the week before the deadline?", "find all decisions related to the propulsion system." Raven searches the causal graph and returns relevant nodes — not just keyword matches, but structurally related events.

**Encryption at rest**
All chain data encrypted with AES-256 via SQLCipher. Keys stored in your OS secure keychain. Optional high-security mode keeps all execution state in RAM only — nothing written to disk except your encrypted chain.

**Hardware options (future)**
For users who want a dedicated always-on device running locally with no laptop required, Raven will support deployment on:

| Device | RAM | Best model | Speed | Price |
|---|---|---|---|---|
| Any laptop/desktop | 8GB+ | Qwen3.5-4B | 10–25 tok/s | — |
| Beelink SER8 | 32GB | Qwen3.5-9B | 15–25 tok/s | ~$450 |
| Raspberry Pi 5 | 8GB | Qwen3.5-2B | 3–5 tok/s | ~$80 |
| Jetson Orin Nano Super | 8GB unified | Qwen3.5-4B | 15–25 tok/s | ~$250 |
| Jetson AGX Orin | 64GB | Qwen3.5-9B | 20–40 tok/s | ~$1000 |

The Beelink SER8 with 32GB is the recommended value option — runs Qwen3.5-9B comfortably, upgradeable RAM, OCuLink port for future GPU expansion. The Jetson Orin Nano Super is recommended when ROS2 and robotics integration are needed. A full hardware deployment guide will be published when Raven reaches that stage.

---

## Getting started

```bash
git clone https://github.com/yourhandle/raven
cd raven
pip install -r requirements.txt
python test_full_system.py
```

The test suite runs 13 end-to-end verification tests covering:
- DAG core, persistence, rollback
- Parallel branching and automatic merging
- Session continuity across process restarts
- LangGraph interrupt/resume with human approval
- Multi-agent subagent spawn and merge
- Concurrent write safety under race conditions
- Conflict detection and store integrity

All 13 tests verified on NVIDIA H100 with Qwen3.5-4B.

To use Raven with a real model, run Qwen3.5-4B locally via Ollama and point Raven at `localhost:11434`. Full integration guide coming soon.

---

## Project status

Raven's core is solid and fully tested on GPU. The DAG, persistence layer, branching, merging, rollback, session reconciliation, and multi-agent support all work and are verified.

Active development:
- Speculative node planning (DAG V2)
- Voice interface (Whisper + Piper TTS + wake word)
- MCP integrations (email, web, payments, smart home)
- Visual observation (Qwen3.5-4B vision at session start)
- Richer relationships and causal reasoning across the chain (RGAT)
- Encryption at rest (SQLCipher + OS keychain)
- Robotics control (ROS2 MCP + YOLO vision)

---

## Philosophy

Most AI memory systems try to solve the wrong problem. They try to make the AI *remember* the way a human remembers — continuously, in the background, always on.

That's hard, expensive, and fragile.

Raven takes a simpler view: **the AI doesn't need to remember. It needs access to a reliable record of what happened.** The chain is that record. It's always accurate, always queryable, always there. The AI reads it at the start of each session and is oriented instantly.

This is how the best human collaborators work too. They don't rely on memory alone — they keep a project log, a decision record, a notebook. Raven is that notebook, structured and queryable, built into the agent's workflow from the start.

Ravens remember. So does your agent.

---

*Built for agents that work on real things, over real time.*
