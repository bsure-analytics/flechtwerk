---
opener: true
title: API Reference
tagline: The vocabulary of the loom — set down here exactly as the source declares it.
---

# API Reference

Flechtwerk's public surface is small and settled in shape. Every entry below is generated directly from the source docstrings, so it never drifts from the code. The core vocabulary is `Extractor`, `Transformer`, `Message`, `State`, `Event`, `Config`, `ConfigStore`, the runtime handle `Flechtwerk`, and the typed-record handles of `flechtwerk.attribute` — with a few supporting types (`Payload`, `IncomingMessage`, `MqttBrokerConfig`, `CompressionType`) documented alongside them below.

## Stages

::: flechtwerk.Transformer

::: flechtwerk.transformer.transformer

::: flechtwerk.Extractor

::: flechtwerk.extractor.extractor

## Records &amp; Messages

::: flechtwerk.Event

::: flechtwerk.State

::: flechtwerk.Config

::: flechtwerk.Message

::: flechtwerk.Payload

::: flechtwerk.IncomingMessage

## Typed Attributes

::: flechtwerk.attribute.Attribute

## Runtime &amp; Configuration

::: flechtwerk.Flechtwerk

::: flechtwerk.ConfigStore

::: flechtwerk.MqttBrokerConfig

::: flechtwerk.CompressionType

## Secrets

The `flechtwerk[secrets]` optional extra — field-level encryption for attributes. See the [Encrypted Secrets](../concepts/secrets.md) concept page for usage and the full spec (wire format, rotation, migration, post-quantum posture).

::: flechtwerk.secrets.ENCRYPTED

::: flechtwerk.keyring.Keyring

::: flechtwerk.secrets.encrypt_value

::: flechtwerk.secrets.reencrypt

::: flechtwerk.secrets.is_encrypted

::: flechtwerk.secrets.kid_of

::: flechtwerk.secrets.scan_config_topics
