---
opener: true
title: Guides
tagline: Practical paths from an empty file to a stage running against a live broker.
---

# Guides

The reference explains what each piece *is*. These guides show you how to *use*
them — task-focused, worked end to end, and safe to copy from.

Start with **Getting started**: install the framework, learn the two-yield
contract, and run a complete stage against any Kafka broker with a single call.
From there the guides branch out by stage shape — **Extractors** bring an external
source into Kafka (with **MQTT Extractors** as the push-driven variant), and
**Transformers** consume topics and publish derived records with exactly-once
delivery. **Best practices** then shows how to combine an extractor and a
transformer so you can reprocess without re-ingesting, and **Observability**
covers the Prometheus metrics. The concept pages fill in the rest: typed
attributes and records, config topics as shared lookup tables, and exactly-once
delivery.

!!! tip "The Whole Contract Is Two `yield` Statements"

    Everything here is built on one idea: a stage is an async generator that
    `yield`s a `Message` to emit a record and `yield`s a `State` to persist
    state for the current key. No agents, no tables, no DSL. If you keep that
    shape in mind, every guide below is a variation on it.
