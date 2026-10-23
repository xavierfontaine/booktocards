---
name: tdd
description: The red-green-refactor cycle and its rules for this repo, including how to fix a bug. Use this when implementing or changing behavior.
---

# Test-driven development

Adapted from [Kent Beck's CLAUDE.md](https://github.com/KentBeck/BPlusTree3/blob/main/rust/docs/CLAUDE.md).

## Cycle

1. **Red**: write one failing test for the next small increment of behavior.
   Run it and check that it fails for the expected reason.
2. **Green**: write the least code that makes it pass. Run all tests.
3. **Refactor**: once all tests pass, improve the structure while they stay
   green. Make one change at a time and run the tests after each.

## Rules

- Name each test after the behavior it checks, such as
  `test_adding_known_token_keeps_one_card`.
- Start from the functional test. The `functional-testing` skill says when to
  add and delete implementation-detail tests.
- Separate structural changes from behavioral changes:
  - Structural: renaming, extracting, moving code. The behavior does not change.
  - Behavioral: new or changed functionality, or a bug fix.
- Run the tests before and after each structural change to show that the
  behavior did not change.

## Fixing a bug

1. Write a failing functional test that reproduces the bug.
2. If the cause is unclear, write the smallest test that reproduces it.
3. Make both tests pass.
