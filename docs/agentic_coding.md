# Agentic coding

This document sets who owns what between the human and the agents, how work
flows, and which files carry it.

## Ownership

| Owner | Owns |
|---|---|
| Human | The end goal. The repo-level functional requirements. The general code architecture. The final say on `ROADMAP.md` and `ARCHITECTURE.md`. The validation of each task file. The merge into `main`. |
| Agent PM | Drafting `ROADMAP.md` and `ARCHITECTURE.md` with the human. Writing `CONCEPTS.md`, `RESEARCH.md` and the task files. Spawning Developers and the Validator. Merging their work. Editing the shared files. |
| Agent Developer | Implementing one task, following the task file. Writing the task's handover. |
| Agent Validator | Running the CI. Non-deterministic testing. Evaluating the quality of the result. |

## Development process

The repo is a partial solution to an end goal. Each epic gets one roadmap,
replaced when the epic ends.

1. The human defines the end goal, with the PM's help.
2. The human defines the repo-level functional requirements: what this repo
   implements toward the end goal.
3. The human defines the general code architecture.
4. The PM records the end goal and the requirements in `ROADMAP.md`, the
   architecture in `ARCHITECTURE.md`, and the repo terms in `CONCEPTS.md`. The
   human approves `ROADMAP.md` and `ARCHITECTURE.md`.
5. The PM writes the task file of the next roadmap part.
6. The human validates the task file. Coding starts only after this.
7. The PM spawns one Developer per task, each in its own git worktree.
8. Each Developer implements its task, writes its handover, and reports the
   handover path to the PM.
9. The PM merges each worktree, applies the proposed updates from the
   handover, and marks the task done.
10. Once all tasks of the part are done, the PM spawns the Validator on the
    merged result. The PM turns each finding into a new task, or reports it to
    the human.
11. The human reviews the result and merges it into `main`.
12. The PM moves to the next roadmap part, from step 5.

### Stop and ask

When an agent lacks knowledge it needs, or cannot access an important
resource, it stops and asks. It does not guess the content or work around it.
The PM asks the human. A Developer or the Validator reports the question to
the PM.

### Avoiding write conflicts

Developers work in parallel, so:

- Only the PM edits the shared files: `CONCEPTS.md`, `ROADMAP.md`,
  `ARCHITECTURE.md`, `RESEARCH.md` and the task files.
- A Developer writes only its code, its tests and its own handover. It puts
  the changes it wants in shared files under "Proposed updates" in its
  handover.
- Each Developer runs in its own git worktree, and the PM merges the worktrees
  one at a time.

## Project files

### `CONCEPTS.md`

- **Purpose**: defines the important concepts of the repo, such as "token",
  "source" or "knowledge base". Code is named after these concepts.
- **Owner**: the PM.
- **Updated**: when a roadmap or a task introduces a concept.
- **Outline**: one `###` heading per concept, with a one-paragraph definition
  and its name in code.

### `ROADMAP.md`

- **Purpose**: describes the roadmap of the current epic.
- **Owner**: the PM and the human. The human has the final say.
- **Updated**: at the start of an epic, then when a part is done or the plan
  changes. Replaced when the epic ends.
- **Outline**:
  - End goal.
  - Repo-level functional requirements.
  - Roadmap parts, in order, each with its goal and a link to its task file.

### `ARCHITECTURE.md`

- **Purpose**: describes the general code architecture: the modules, their
  responsibilities, and how data flows between them.
- **Owner**: the PM and the human. The human has the final say.
- **Updated**: when a roadmap changes the architecture.
- **Outline**: one `###` heading per module, then a section on data flow.

### `RESEARCH.md`

- **Purpose**: stores the knowledge accumulated about the problem domain, the
  solution domain and the implementation domain, so that later agents do not
  redo the research.
- **Owner**: the PM. Other agents propose additions through the PM.
- **Updated**: whenever an agent learns something a later agent would need.
- **Outline**: one `##` heading per domain. Each finding states the fact, then
  its source.

### `docs/tasks/<part>.md`

- **Purpose**: splits one roadmap part into tasks. `<part>` is a short
  kebab-case name, such as `01-on-the-fly-tokens`.
- **Owner**: the PM. The human validates it before coding starts.
- **Updated**: when the part starts, then when a task is done.
- **Outline**:
  - Goal of the part, and a link to its section in `ROADMAP.md`.
  - Status: `draft`, `validated` or `done`.
  - One `###` heading per task, named `<task-id>: <title>` such as
    `T1: Count studiable tokens`, with:
    - Goal.
    - Acceptance criteria, as units of functionality.
    - Files it touches.
    - Tasks it depends on.
    - Status: `todo`, `in progress` or `done`.
- **Rule**: as much as possible, write tasks that Developers can implement
  independently, touching separate files.

### `docs/tasks/<part>/<task-id>_HANDOVER.md`

- **Purpose**: records how a Developer implemented a task.
- **Owner**: the Developer that implemented the task.
- **Updated**: once, when the task is done.
- **Outline**:
  - What I did.
  - Decisions I took, each with its reason.
  - Differences with the task file.
  - Proposed updates to shared files.
  - Open questions.

## Agents

Each agent is defined in `.claude/agents/`.

- **Agent PM** (`.claude/agents/pm.md`) turns the human's goals into a
  roadmap and tasks, then runs the Developers and the Validator. It runs as
  the main session with `claude --agent pm`, because only the main session can
  spawn agents.
- **Agent Developer** (`.claude/agents/developer.md`) implements one task with
  test-driven development, following the `tdd` and `functional-testing`
  skills.
- **Agent Validator** (`.claude/agents/validator.md`) checks a merged roadmap
  part and reports findings. It does not fix them.
