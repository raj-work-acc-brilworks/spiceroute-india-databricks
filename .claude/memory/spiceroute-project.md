---
name: spiceroute-project
description: "SpiceRoute India Databricks project in db-prac — workspace/profile, catalog, deployed asset IDs, GitHub repo, no-Claude-attribution rule"
metadata:
  node_type: memory
  type: project
  originSessionId: 55da654f-1185-460a-8341-d0d8ac16c763
  modified: 2026-10-05T14:10:08.230Z
---

SpiceRoute India is an end-to-end Databricks Free Edition retail project in `/home/brilworks/Downloads/db-prac`. It is a DAB bundle `spiceroute_india` covering synthetic data (25M lines), medallion pipeline, metric views, hierarchical Prophet forecast, dashboard, Genie space and Streamlit app. Built 2026-10-05; the repo's `CLAUDE.md` holds the full conventions.

- Profile `brilworks` (dedicated workspace dbc-821c89ac-7917). The `raj.s` profile holds an unrelated retail POC; never touch it.
- Catalog `spiceroute`; warehouse `9cd430b8a1739112`; dashboard `01f1c0bd2fb31f4fa957edeceee74365`; Genie `01f1c0bd7bd6175ba0b691784fd34971`; app `spiceroute-demand-planner`.
- Pushed to GitHub as a public repo under account `raj-work-acc-brilworks`.

**Why:** the user wants an isolated demo project, run manually to save the Free Edition quota.
**How to apply:**
- always use `--profile brilworks` and run jobs manually, with no schedules;
- regenerate the dashboard and Genie from their builder scripts;
- see [[no-claude-attribution]] before committing.
