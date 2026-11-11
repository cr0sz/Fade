Analyze these files and produce GraphNode and GraphEdge objects.
Project root: `C:/Users/bekir/Documents/projeler/Fade`
Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`
Batch: `19/24`
Skill directory (for bundled scripts): `C:/Users/bekir/.claude/plugins/cache/understand-anything/understand-anything/2.9.7/skills/understand`
Data directory ($UA_DIR): `C:/Users/bekir/Documents/projeler/Fade/.ua`
Output: write to `C:/Users/bekir/Documents/projeler/Fade/.ua/intermediate/batch-19.json` (single-file mode) OR `batch-19-part-<k>.json` (split mode, per Step B of your output protocol).

Pre-resolved import data for this batch (use directly — do NOT re-resolve imports from source):
```json
{"monitoring/homepage/bookmarks.yaml":[],"monitoring/homepage/custom.css":[],"monitoring/homepage/docker.yaml":[],"monitoring/homepage/index.html":[],"monitoring/homepage/kubernetes.yaml":[],"monitoring/homepage/proxmox.yaml":[],"monitoring/homepage/services.yaml":[],"monitoring/homepage/settings.yaml":[],"monitoring/homepage/widgets.yaml":[]}
```

Cross-batch neighbors with their exported symbols (confidence boost for cross-batch edges):
```json
{}
```

Files to analyze in this batch (every entry MUST be passed through to `batchFiles` with all four fields — `path`, `language`, `sizeLines`, `fileCategory`):
1. `monitoring/homepage/bookmarks.yaml` (7 lines, language: `yaml`, fileCategory: `config`)
2. `monitoring/homepage/custom.css` (429 lines, language: `css`, fileCategory: `markup`)
3. `monitoring/homepage/docker.yaml` (10 lines, language: `yaml`, fileCategory: `config`)
4. `monitoring/homepage/index.html` (540 lines, language: `html`, fileCategory: `markup`)
5. `monitoring/homepage/kubernetes.yaml` (2 lines, language: `yaml`, fileCategory: `config`)
6. `monitoring/homepage/proxmox.yaml` (5 lines, language: `yaml`, fileCategory: `config`)
7. `monitoring/homepage/services.yaml` (72 lines, language: `yaml`, fileCategory: `config`)
8. `monitoring/homepage/settings.yaml` (23 lines, language: `yaml`, fileCategory: `config`)
9. `monitoring/homepage/widgets.yaml` (19 lines, language: `yaml`, fileCategory: `config`)

**Additional context from main session:**

Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`

> **Language directive**: Generate all textual content (summaries, descriptions, tags, titles, languageNotes, languageLesson) in **English**. Maintain technical accuracy while using natural, native-level phrasing in the target language. Keep technical terms in English when no standard translation exists (e.g., "middleware", "hook", "barrel").

Never read `.env` or any secrets file; it is not in your batch.
