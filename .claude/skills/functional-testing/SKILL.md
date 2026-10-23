---
name: functional-testing
description: How this repo writes tests - units of functionality, functional versus implementation-detail tests, and the `_` naming rule. Use this when writing, deleting or reviewing tests.
---

# Testing

This repository relies on functional testing.

## Units of functionality

- A unit of functionality is a behavior the user of the code cares about, such
  as "adding a token from a source creates one card per new kanji". It is not a
  class or a function.
- The developer decides where the units lie. Before writing tests, list the
  units you propose, each in one sentence, and ask the developer to confirm or
  correct them.

## Test levels

- **Functional tests** check a unit of functionality through its public entry
  point. They form the center of the suite.
- **Implementation-detail tests** check one component that a functional test
  needs. Write one only when making the functional test pass is too large a
  step.
- Implementation-detail tests are temporary. Delete them once a functional test
  covers their component.

## The `_` rule

- A class or function without its own test starts with `_`.
- Existing code predates this rule. Apply it to the code you add or change.
- When you delete an implementation-detail test, rename its component with a
  leading `_`, unless another test still targets it directly.
- The public names of a module then list the units of functionality it offers.
