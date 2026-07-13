---
opener: true
title: Guides
tagline: Practical paths from an empty file to a stage running against a live broker.
---

# Guides

The reference explains what each piece *is*. These guides show you how to *use*
them — task-focused, worked end to end, and safe to copy from.

Start with **Getting started**: install the framework, write a minimal
`Transformer`, and run it against any Kafka broker with a single call. From
there the guides branch out from general to specific — the **Extractor** guide
picks up the same two-yield contract from the other end (poll an external source
instead of consuming a topic), and **MQTT Extractor** specializes that further
into a push-driven source. The concept pages fill in the rest: stateful
processing, config topics as shared lookup tables, and exactly-once delivery.

!!! tip "The Whole Contract Is Two `yield` Statements"

    Everything here is built on one idea: a stage is an async generator that
    `yield`s a `Message` to emit a record and `yield`s a `State` to persist
    state for the current key. No agents, no tables, no DSL. If you keep that
    shape in mind, every guide below is a variation on it.
