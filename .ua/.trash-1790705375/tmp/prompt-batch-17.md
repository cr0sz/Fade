Analyze these files and produce GraphNode and GraphEdge objects.
Project root: `C:/Users/bekir/Documents/projeler/Fade`
Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`
Batch: `17/24`
Skill directory (for bundled scripts): `C:/Users/bekir/.claude/plugins/cache/understand-anything/understand-anything/2.9.7/skills/understand`
Data directory ($UA_DIR): `C:/Users/bekir/Documents/projeler/Fade/.ua`
Output: write to `C:/Users/bekir/Documents/projeler/Fade/.ua/intermediate/batch-17.json` (single-file mode) OR `batch-17-part-<k>.json` (split mode, per Step B of your output protocol).

Pre-resolved import data for this batch (use directly — do NOT re-resolve imports from source):
```json
{".env.example":[],"CLAUDE.md":[],"Makefile":[],"README.md":[],"docker-compose.yml":[],"requirements-dev.txt":[],"requirements.txt":[]}
```

Cross-batch neighbors with their exported symbols (confidence boost for cross-batch edges):
```json
{}
```

Files to analyze in this batch (every entry MUST be passed through to `batchFiles` with all four fields — `path`, `language`, `sizeLines`, `fileCategory`):
1. `.env.example` (491 lines, language: `config`, fileCategory: `config`)
2. `CLAUDE.md` (25 lines, language: `markdown`, fileCategory: `docs`)
3. `Makefile` (108 lines, language: `makefile`, fileCategory: `infra`)
4. `README.md` (239 lines, language: `markdown`, fileCategory: `docs`)
5. `docker-compose.yml` (944 lines, language: `yaml`, fileCategory: `infra`)
6. `requirements-dev.txt` (6 lines, language: `txt`, fileCategory: `docs`)
7. `requirements.txt` (46 lines, language: `txt`, fileCategory: `docs`)

**Additional context from main session:**

Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`
Languages: `config, css, dockerfile, html, javascript, json, makefile, markdown, python, shell, sql, txt, unknown, yaml`

> **Language directive**: Generate all textual content (summaries, descriptions, tags, titles, languageNotes, languageLesson) in **English**. Maintain technical accuracy while using natural, native-level phrasing in the target language. Keep technical terms in English when no standard translation exists (e.g., "middleware", "hook", "barrel").

Never read `.env` or any secrets file; it is not in your batch.
