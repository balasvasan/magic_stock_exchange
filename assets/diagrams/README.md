# ARGUS — Diagram Index

Nine diagram-sets covering the entire capstone arc, each in two formats:

- **SVG** — embeds in `capstone.html` (the Pages site); renders sharp at any zoom
- **Mermaid** — renders inline in GitHub's Markdown viewer (no clicks needed)

Each diagram is one style, chosen to fit the content. See [Project ARGUS PRD §17](../../ARG_prd_v1.md) for the locked naming convention referenced in every diagram.

## The 9 diagrams

| # | Diagram | Style | What it answers | Files |
|---|---|---|---|---|
| 0 | **Master Overview** | Timeline (Gantt) + arch zones | "Where am I in the 10 days?" | [SVG](00_master_overview.svg) · [MD](00_master_overview.md) |
| 1 | **Day 1 — Setup** | Activity (4 swim lanes) | "What do I do today?" | [SVG](01_day1_setup.svg) · [MD](01_day1_setup.md) |
| 2 | **Module 1 — CDF + Flink + SSB** | Sequential pipeline (3-engine fanout) | "Where does data flow + which engine catches what?" | [SVG](02_module1_streaming.svg) · [MD](02_module1_streaming.md) |
| 3 | **Module 2 — Identity + Book** | Two parallel pipelines | "How do we resolve identity + reconstruct the book?" | [SVG](03_module2_cde_identity.svg) · [MD](03_module2_cde_identity.md) |
| 4 | **Module 3 — Features + Rules** | Sequential pipeline (5 rules) | "Which rule fires for which case?" | [SVG](04_module3_cde_features.svg) · [MD](04_module3_cde_features.md) |
| 5 | **Module 4 — Governed Views** | Architecture-zoom (3 roles) | "Why do these 3 users see different things?" | [SVG](05_module4_cdw_governed.svg) · [MD](05_module4_cdw_governed.md) |
| 6 | **Module 5 — ML / MLflow** | Activity (5 phases + manual gate) | "How does training reach Production?" | [SVG](06_module5_cml_ml.svg) · [MD](06_module5_cml_ml.md) |
| 7 | **Module 6 — GenAI / RAG** | Sequential pipeline (setup + per-alert) | "How does an STR draft get generated?" | [SVG](07_module6_cml_genai.svg) · [MD](07_module6_cml_genai.md) |
| 8 | **Module 7 — SDX Governance** | Activity (3 sub-flows + CP-19 gate) | "How do we prove DPDP §12 compliance?" | [SVG](08_module7_sdx_governance.svg) · [MD](08_module7_sdx_governance.md) |

## How to use these

**For day-to-day reference:** open the relevant module's `.md` file in GitHub's web UI — Mermaid renders inline.

**For instructor presentations or printed handouts:** open the `.svg` file in a browser, "Save As" PDF, or right-click → Save the rendered image. The SVGs are designed for 1400×~700–880 viewBox so they print cleanly at letter or A4 landscape.

**For embedding in your own docs:** the SVGs are self-contained (one file each, no external assets). Copy them anywhere; they'll just work.

## Visual language

Consistent across all 9 diagrams:

- **Cloudera orange `#f96302`** — student work, capstone systems, what you're building
- **Indigo `#6366f1`** — compliance gates, CP-19, and anywhere DPDP enforcement is at play
- **Slate gray** — instructor-provisioned or external resources (TARANG, NIPATAN, Kafka cluster, MLflow registry)
- **Red dashed `#ef4444`** — DLQ paths, failure routes
- **Indigo dashed border** — manual decision gates (DPO approval in Module 5, CP-19 in Module 7)

A circle with `00`, `01`, ... `20` always denotes a checkpoint. CP-19 alone is rendered as an indigo filled circle to flag its COMPLIANCE GATE status.

## See also

- [PRD §17 — Naming Convention](../../ARG_prd_v1.md) — `${STUDENT_ID}` substitution rules
- [Architecture overview SVG](../architecture.svg) — the 4-layer architecture diagram referenced from `capstone.html`
- [Lab 0.1 — Environment Provisioning](../../labs/lab-0-1-environment-provisioning.md) — Day 1 walkthrough that this diagram set illustrates
