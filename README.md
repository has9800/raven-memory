# TCC — Temporal Causal Chain

**Your AI agent finally remembers.**

Most AI agents forget everything the moment you close the window. TCC fixes that. It gives any AI agent a persistent, structured memory that survives across sessions, days, and weeks — so the next time you sit down to work, your agent already knows what you were doing, what decisions were made, and what comes next.

---

## The problem with AI memory today

Every AI assistant you've used has the same fundamental flaw: it only knows what's in the current conversation. Close the chat, start a new one, and it's a blank slate. You spend the first five minutes re-explaining your project, your context, your preferences.

Some tools try to fix this with notes files or conversation logs. But these are flat — just a pile of text with no understanding of cause and effect, no sense of what led to what, no way to go back and undo a decision.

TCC takes a different approach.

---

## How TCC works

TCC records everything that happens as a **chain of events**, where each event knows what caused it.

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

When you come back tomorrow, TCC loads the end of that chain and hands it to your agent. The agent reads it and is instantly oriented — no re-explaining, no catching up, no lost context.

---

## What makes it a DAG, not just a list

A simple list of events would be fine for linear work. But real projects don't happen linearly. You run a simulation *while* turning off the lights. You explore two approaches in parallel before committing to one. You sometimes realize a decision was wrong and need to go back.

TCC uses a **Directed Acyclic Graph** (DAG) — a structure that can represent all of this cleanly:

**Branching** — when two things happen in parallel, the chain forks into two branches that run side by side.

**Merging** — when both parallel tasks finish, the branches automatically merge back into one chain. The agent sees a single coherent history.

**Rollback** — made a wrong decision? Roll the chain back to any earlier point. Nothing is deleted — the history is preserved, but the agent's current view moves back to a known-good state.

**Speculative nodes** — coming soon: the agent can plan ahead by adding *speculative* nodes representing what it thinks will happen next. As reality unfolds, nodes get confirmed or pruned. The agent maintains a live hypothesis about the future.

---

## Persistence that actually works

TCC stores everything in a local SQLite database — a single file on your machine. No cloud, no subscription, no data leaving your computer.

When you start a new session:

1. TCC loads the last N events from the chain
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
Run multiple tasks at the same time. TCC tracks both, and automatically merges them back into a single coherent timeline when they're done.

**Rollback to any point**
Changed your mind? Roll the chain back to any previous state. The agent picks up from there as if nothing went wrong. History is preserved — nothing is lost.

**Session-based physical state**
TCC doesn't try to track where every physical object is in real time. Instead, it records what the agent knows at the moment — and re-observes the physical world at the start of each session. Simple, honest, robust.

**Local first, privacy first**
Everything lives in a single SQLite file on your machine. No cloud, no accounts, no data leaving your computer. Run it on a Raspberry Pi, a Mac Mini, a server — anything.

**Model agnostic**
TCC works with any language model. Swap between models, upgrade to a newer one, run locally or via API — the memory layer doesn't change.

**Cold storage for old history**
Configure how long events stay in active memory. Older nodes get compressed and archived automatically. A year of daily activity fits in a few megabytes.

**Speculative planning** *(coming soon)*
The agent can plan ahead by projecting future events onto the chain. As reality unfolds, speculative nodes get confirmed or pruned. The agent maintains a live hypothesis about what comes next.

**Graph-based search** *(coming soon)*
Ask questions about your history: "why did we decide X", "what was happening on March 2nd", "find all decisions related to the propulsion system." TCC searches the graph and returns relevant nodes, not just keyword matches.

---

## Example: picking up after a three-week break

You come back to a project you haven't touched in three weeks.

**Without TCC:**
You open a new chat. The agent knows nothing. You spend ten minutes explaining what you're building, what you tried, what didn't work, what comes next. You probably miss something important. The agent makes a suggestion that contradicts a decision you already made — because it doesn't know you made it.

**With TCC:**
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

## Getting started

```bash
git clone https://github.com/yourhandle/tcc
cd tcc
pip install -r requirements.txt
python test_full_system.py
```

The test suite runs 8 end-to-end verification tests covering persistence, parallel branching, automatic merging, rollback, and session continuity — all without needing a GPU or API key.

To use TCC with a real model, point it at any local or API-based language model and pass the session context into your system prompt. Full integration guide coming soon.

---

## Project status

TCC is early but the core is solid and tested. The DAG, persistence layer, branching, merging, rollback, and session reconciliation all work. Integration with tool-calling agents is working and verified on GPU.

Active development areas:
- Speculative node planning
- Semantic search over chain history
- ROS2 bridge for physical robot tool calls
- VLM-based session observation (agent opens its eyes at session start)
- Vector store integration for similarity-based history retrieval

---

## Philosophy

Most AI memory systems try to solve the wrong problem. They try to make the AI *remember* the way a human remembers — continuously, in the background, always on.

That's hard, expensive, and fragile.

TCC takes a simpler view: **the AI doesn't need to remember. It needs access to a reliable record of what happened.** The chain is that record. It's always accurate, always queryable, always there. The AI reads it at the start of each session and is oriented instantly.

This is how the best human collaborators work too. They don't rely on memory alone — they keep a project log, a decision record, a notebook. TCC is that notebook, structured and queryable, built into the agent's workflow from the start.

---

*Built for agents that work on real things, over real time.*
