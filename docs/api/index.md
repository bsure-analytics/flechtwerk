---
opener: true
title: API Reference
tagline: The vocabulary of the loom — set down here exactly as the source declares it.
---

# API Reference

Flechtwerk's public surface is small and settled in shape. Every entry below is generated directly from the source docstrings, so it never drifts from the code. The core vocabulary is `Extractor`, `Transformer`, `Message`, `State`, `Event`, `Config`, `ConfigStore`, the runtime handle `Flechtwerk`, and the typed-record handles of `flechtwerk.attribute` — with a few supporting types (`Payload`, `IncomingMessage`, `InvalidMessageError`, `MqttBrokerConfig`, `CompressionType`) documented alongside them below.

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

::: flechtwerk.InvalidMessageError

## Typed Attributes

::: flechtwerk.attribute.Attribute

::: flechtwerk.attribute.codec
    options:
      filters: ["!^_"]
      show_if_no_docstring: true

## Runtime &amp; Configuration

::: flechtwerk.Flechtwerk

::: flechtwerk.ConfigStore

::: flechtwerk.MqttBrokerConfig

::: flechtwerk.CompressionType

## Secrets

The `flechtwerk[secrets]` optional extra — field-level encryption and signing for attributes. See the [Encrypted Secrets](../concepts/secrets.md) concept page for usage and the full spec (wire format, rotation, migration, post-quantum posture). The keyring classes and the token primitives live in the [`flechtctl`](#flechtctl) package, which the extra installs, and are importable from `flechtwerk.keyring` and `flechtwerk.secrets` too — they are the same objects.

::: flechtwerk.secrets.ENCRYPTED

::: flechtwerk.secrets.SIGNED

::: flechtctl.keyring.Keyring

::: flechtctl.keyring.X25519Key

::: flechtctl.keyring.Ed25519Key

::: flechtwerk.secrets.encrypt_value

::: flechtwerk.secrets.sign_value

::: flechtwerk.secrets.reencrypt

::: flechtwerk.secrets.scan_config_topics

## flechtctl

The standalone `flechtctl` distribution — keyrings and secret tokens over an explicit keyring, plus the `flechtctl` command. It depends on joserfc only and never imports Flechtwerk. See the guide [Keyrings and Secret Tokens with flechtctl](../guides/flechtctl.md). The entries below are documented at their defining modules; `flechtctl` re-exports every name, and `flechtwerk.keyring` / `flechtwerk.secrets` re-export most of them.

### Keyring files

::: flechtctl.keyring.keyring_from_jwk

::: flechtctl.keyring.is_encrypted_keyring

::: flechtctl.tokens.encrypt_keyring

::: flechtctl.tokens.decrypt_keyring

### Values and tokens

::: flechtctl.tokens.encrypt

::: flechtctl.tokens.decrypt

::: flechtctl.tokens.sign

::: flechtctl.tokens.verify

::: flechtctl.tokens.reencrypt

::: flechtctl.tokens.encrypt_bytes

::: flechtctl.tokens.decrypt_bytes

::: flechtctl.tokens.sign_bytes

::: flechtctl.tokens.verify_bytes

::: flechtctl.tokens.read_header

::: flechtctl.tokens.is_encrypted

::: flechtctl.tokens.is_signed

::: flechtctl.tokens.kid_of

::: flechtctl.tokens.signer_of

::: flechtctl.tokens.canonical_json

### Key generation

::: flechtctl.tokens.generate_oct_key

::: flechtctl.tokens.generate_x25519_key

::: flechtctl.tokens.generate_ed25519_key

### Errors

::: flechtctl.errors.SecretError

::: flechtctl.errors.SecretFormatError

::: flechtctl.errors.SecretDecryptError

::: flechtctl.errors.PlaintextSecretError

::: flechtctl.errors.SecretSignatureError

::: flechtctl.errors.UnsignedSecretError

::: flechtctl.keyring.KeyringError

::: flechtctl.keyring.UnknownKeyError
