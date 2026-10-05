---
name: no-claude-attribution
description: "User forbids Claude co-author/attribution lines in commits, PRs or files for this project"
metadata:
  node_type: memory
  type: feedback
  originSessionId: 55da654f-1185-460a-8341-d0d8ac16c763
  modified: 2026-10-05T14:10:08.422Z
---

Never add "Co-Authored-By: Claude …" or any other Claude/AI attribution to commit messages, PR descriptions or repo files in this project.

**Why:** the user explicitly said "ensure don't add co work by claude anywhere" before publishing the public GitHub repo.
**How to apply:** this instruction overrides the default attribution reminder for every commit and PR in [[spiceroute-project]].
