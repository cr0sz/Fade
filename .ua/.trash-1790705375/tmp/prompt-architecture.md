Analyze this codebase's structure to identify architectural layers.
Project root: `C:/Users/bekir/Documents/projeler/Fade`
Write output to: `C:/Users/bekir/Documents/projeler/Fade/.ua/intermediate/layers.json` (plain JSON array of layers: {id, name, description, nodeIds})
Project: `Fade` — `An AI-driven market intelligence pipeline that ingests news, social, and smart-money data, turns it into scored trading signals, validates those signals against real price outcomes, and — once a signal has proven itself — routes it into a paper or live trading engine (crypto via Binance, equities via Alpaca) with a hard safety layer in front of any real money.`

Input data (READ this file; it contains the three JSON blocks the agent definition expects):
`C:/Users/bekir/Documents/projeler/Fade/.ua/tmp/arch-input.json` = { "fileNodes": [{id,type,name,filePath,summary,tags} for every file-level node — 398 nodes], "importEdges": [edges of type imports], "allEdges": [all non-contains edges between file-level nodes] }.
Every file-level node id in fileNodes MUST appear in exactly one layer's nodeIds. Use node ids exactly as given.

## Language Context

### css
# CSS Language Prompt Snippet

## Key Concepts

- **Selectors**: Element, class (`.name`), ID (`#name`), attribute (`[attr]`), and pseudo-class (`:hover`) targeting
- **Specificity**: Inline > ID > Class > Element cascade priority determining which rules win
- **Box Model**: `margin`, `border`, `padding`, `content` dimensions controlling element sizing
- **Flexbox**: `display: flex` with `justify-content`, `align-items` for one-dimensional layouts
- **Grid**: `display: grid` with `grid-template-columns/rows` for two-dimensional layouts
- **Custom Properties (Variables)**: `--name: value` with `var(--name)` for reusable design tokens
- **Media Queries**: `@media (max-width: ...)` for responsive design breakpoints
- **SCSS/Sass Features**: Nesting, `$variables`, `@mixin`, `@include`, `@extend`, `@use`, `@forward`
- **CSS Modules**: Scoped class names (`.module.css`) preventing global style collisions
- **Cascade Layers**: `@layer` for explicit control over cascade ordering

## Notable File Patterns

- `*.css` — Standard CSS stylesheets
- `*.scss` / `*.sass` — Sass/SCSS preprocessor files
- `*.less` — Less preprocessor files
- `*.module.css` / `*.module.scss` — CSS Modules (scoped styles)
- `globals.css` / `reset.css` / `normalize.css` — Global base styles
- `tailwind.config.js` — Tailwind CSS configuration (though a JS file)
- `variables.scss` / `_variables.scss` — Design token definitions

## Edge Patterns

- CSS files are `related` to the HTML or component files that import them for styling
- SCSS partial files (`_*.scss`) are `depends_on` by the main stylesheet that `@use`s them
- CSS variable definition files are `related` to all stylesheets that reference those variables
- CSS Modules are `related` to the component files that import them

## Summary Style

> "Global stylesheet defining CSS custom properties for the design system color palette and typography."
> "Responsive layout styles with flexbox and grid for the dashboard page across 3 breakpoints."
> "SCSS partial defining shared mixins for spacing, shadows, and media query breakpoints."

### dockerfile
# Dockerfile Language Prompt Snippet

## Key Concepts

- **Multi-Stage Builds**: Multiple `FROM` statements to separate build and runtime stages, reducing image size
- **Layer Caching**: Each instruction creates a layer; order instructions from least to most frequently changing for cache efficiency
- **Base Images**: `FROM image:tag` selects the starting image; prefer slim/alpine variants for smaller images
- **COPY vs ADD**: `COPY` for local files (preferred), `ADD` for URLs and tar extraction
- **Build Arguments**: `ARG` for build-time variables, `ENV` for runtime environment variables
- **Health Checks**: `HEALTHCHECK` instruction for container orchestrator readiness probes
- **Entry Point vs CMD**: `ENTRYPOINT` sets the executable, `CMD` provides default arguments
- **User Permissions**: `USER` instruction to run as non-root for security
- **Ignore Patterns**: `.dockerignore` excludes files from the build context (like `.gitignore`)

## Notable File Patterns

- `Dockerfile` — Primary container image definition (at project root)
- `Dockerfile.dev` / `Dockerfile.prod` — Environment-specific Dockerfiles
- `docker-compose.yml` — Multi-container application orchestration
- `docker-compose.override.yml` — Local development overrides
- `.dockerignore` — Build context exclusion patterns

## Edge Patterns

- Dockerfile `deploys` the application entry point it packages (COPY/CMD target)
- docker-compose `depends_on` Dockerfile(s) it references for building
- Dockerfile `depends_on` package manifests (package.json, requirements.txt) it copies for dependency installation
- docker-compose services create `related` edges between co-deployed components

## Summary Style

> "Multi-stage Docker build producing a minimal Node.js production image with N build stages."
> "Docker Compose configuration orchestrating N services with shared networking and persistent volumes."
> "Development Dockerfile with hot-reload support and mounted source volumes."

### html
# HTML Language Prompt Snippet

## Key Concepts

- **Semantic Elements**: `<main>`, `<nav>`, `<header>`, `<footer>`, `<article>`, `<section>` for meaningful structure
- **Document Structure**: `<!DOCTYPE html>`, `<html>`, `<head>`, `<body>` forming the page skeleton
- **Forms**: `<form>`, `<input>`, `<select>`, `<textarea>` for user data collection with validation attributes
- **Accessibility**: `aria-*` attributes, `role`, `alt` text, and semantic markup for screen readers
- **Meta Tags**: `<meta>` for viewport, charset, description, Open Graph, and SEO metadata
- **Script and Style Loading**: `<script>`, `<link>`, `<style>` for JavaScript and CSS inclusion
- **Data Attributes**: `data-*` custom attributes for storing element-specific data
- **Template Syntax**: Framework-specific templating (`{{ }}` for Jinja/Django, `<%= %>` for ERB)
- **Web Components**: `<template>`, `<slot>`, Custom Elements for encapsulated reusable components

## Notable File Patterns

- `index.html` — Application entry point or SPA shell
- `*.html` / `*.htm` — Static HTML pages
- `templates/**/*.html` — Server-side template files (Django, Jinja2, Go templates)
- `public/index.html` — SPA root document (React, Vue)
- `*.ejs` / `*.hbs` / `*.pug` — Templating engine files

## Edge Patterns

- HTML files `depends_on` JavaScript and CSS files they include via `<script>` and `<link>` tags
- Template HTML files `depends_on` the server-side code that renders them
- HTML entry points are `deploys` targets for build systems and web servers
- HTML files `related` to the components or routes they render

## Summary Style

> "Single-page application shell with viewport meta, CSS reset, and React root mount point."
> "Server-rendered template with navigation, content area, and footer using Django template inheritance."
> "Static landing page with responsive layout, form, and third-party script integrations."

### javascript
# JavaScript Language Prompt Snippet

## Key Concepts

- **Closures**: Functions that capture variables from their enclosing lexical scope
- **Prototypes**: Prototype chain-based inheritance underlying all JavaScript objects
- **Promises**: Asynchronous value containers enabling `.then()` chaining and `async/await`
- **Event Loop**: Single-threaded concurrency model with microtask and macrotask queues
- **Destructuring**: Extract values from objects and arrays into distinct variables
- **Spread/Rest Operators**: `...` for expanding iterables or collecting remaining arguments
- **Proxies**: Meta-programming construct to intercept and customize object operations
- **Generators**: Functions using `function*` and `yield` for lazy iteration
- **Symbol**: Unique, immutable primitive used for non-string property keys
- **WeakMap/WeakSet**: Collections with weakly-held keys allowing garbage collection
- **Modules (ESM vs CJS)**: ES Modules use `import/export`; CommonJS uses `require/module.exports`

## Import Patterns

- `import { X } from 'module'` — ESM named import
- `const X = require('module')` — CommonJS require
- `import('module')` — dynamic import returning a Promise (code splitting)
- `export default X` / `export { X }` — ESM export forms

## File Patterns

- `index.js` — barrel file or directory entry point
- `.mjs` — explicitly ES Module files
- `.cjs` — explicitly CommonJS files
- `package.json` `"type"` field — sets default module system (`"module"` or `"commonjs"`)

## Common Frameworks

- **React** — Declarative UI with virtual DOM and component model
- **Vue** — Progressive framework with reactivity system and single-file components
- **Express** — Minimal and flexible Node.js web application framework
- **Next.js** — React framework for production with hybrid rendering
- **Svelte** — Compile-time framework that shifts work from runtime to build step

## Example Language Notes

> Closure captures outer `config` variable, providing encapsulated state without class
> overhead. The returned object's methods share access to the same `config` reference,
> forming a module pattern that was standard before ES Modules.
>
> When encountering `.mjs` vs `.cjs` extensions, the module system is determined by
> extension regardless of the `package.json` type field — useful in mixed codebases.

### json
# JSON Language Prompt Snippet

## Key Concepts

- **Strict Syntax**: No trailing commas, no comments (unlike JSONC or JSON5), double-quoted strings only
- **Data Types**: Objects, arrays, strings, numbers, booleans, and null — no undefined or date types
- **Nested Structure**: Arbitrary nesting depth for hierarchical configuration or data
- **Schema Validation**: JSON Schema (`$schema` keyword) for validating structure and types
- **JSONC**: JSON with Comments variant used by VS Code, tsconfig.json, and other tooling
- **JSON5**: Extended JSON allowing comments, trailing commas, unquoted keys, and more
- **JSON Lines** (`.jsonl`): One JSON object per line for streaming data processing

## Notable File Patterns

- `package.json` — Node.js project manifest with dependencies, scripts, and metadata
- `tsconfig.json` — TypeScript compiler configuration (actually JSONC)
- `.eslintrc.json` — ESLint linting rules and configuration
- `*.schema.json` — JSON Schema definitions for validation
- `composer.json` — PHP Composer project manifest
- `appsettings.json` — .NET application configuration
- `manifest.json` — Browser extension or PWA manifest

## Edge Patterns

- `package.json` `configures` the build toolchain and defines project dependencies
- `tsconfig.json` `configures` TypeScript compilation for all `.ts` files
- JSON Schema files `defines_schema` for API request/response validation
- Config JSON files `configures` the runtime behavior of the application

## Summary Style

> "Node.js project manifest defining N dependencies, build scripts, and project metadata."
> "TypeScript compiler configuration enabling strict mode with path aliases for monorepo packages."
> "JSON Schema defining the request/response structure for the user API endpoint."

### markdown
# Markdown Language Prompt Snippet

## Key Concepts

- **Heading Hierarchy**: `#` through `######` for document structure, with h1 as the title
- **Front Matter**: YAML metadata between `---` delimiters at the top of the file
- **Fenced Code Blocks**: Triple backticks with optional language identifier for syntax highlighting
- **Reference-Style Links**: `[text][ref]` with `[ref]: url` definitions, useful for repeated URLs
- **Tables**: Pipe-delimited columns with alignment markers (`:---`, `:---:`, `---:`)
- **Admonitions**: Blockquote-based callouts (`> **Note:**`, `> **Warning:**`) for emphasis
- **Task Lists**: `- [ ]` and `- [x]` for checklists in issue trackers and READMEs
- **HTML Embedding**: Raw HTML allowed inline for features Markdown does not support natively

## Notable File Patterns

- `README.md` — Project overview and entry point for new contributors (high-value)
- `CONTRIBUTING.md` — Contribution guidelines, code style, PR process
- `CHANGELOG.md` — Version history following Keep a Changelog or similar format
- `docs/**/*.md` — Documentation directory with guides, API references, tutorials
- `*.md` in source directories — Co-located documentation for modules or packages
- `ADR-*.md` or `adr/*.md` — Architecture Decision Records

## Edge Patterns

- Markdown files `documents` the code components they describe or reference
- Links to other `.md` files create `related` edges between documentation nodes
- Code block references mentioning file paths may imply `documents` edges to those files
- README files in subdirectories typically `documents` the module at that path

## Summary Style

> "Project overview documentation with N sections covering installation, usage, and API reference."
> "Architecture Decision Record documenting the choice of [technology] for [purpose]."
> "Contributing guide with code style rules, testing requirements, and pull request process."

### python
# Python Language Prompt Snippet

## Key Concepts

- **Decorators**: Functions that wrap other functions or classes using `@decorator` syntax
- **List/Dict Comprehensions**: Concise syntax for creating collections from iterables
- **Generators and Yield**: Lazy iterators using `yield` for memory-efficient data processing
- **Context Managers**: `with` statement for resource management via `__enter__`/`__exit__`
- **Type Hints and Typing Module**: Optional static type annotations for tooling and documentation
- **Dunder Methods**: Special methods like `__init__`, `__repr__`, `__eq__` defining object behavior
- **Metaclasses**: Classes that define how other classes are created (type as default metaclass)
- **Dataclasses**: `@dataclass` decorator auto-generating boilerplate from field annotations
- **Protocols**: Structural subtyping via `typing.Protocol` for duck-type-safe interfaces
- **Descriptors**: Objects defining `__get__`, `__set__`, `__delete__` to customize attribute access
- **Async/Await with Asyncio**: Cooperative concurrency using coroutines and an event loop

## Import Patterns

- `from module import name` — import specific name from module
- `import module` — import entire module, access via `module.name`
- `from package.module import name` — absolute import from nested package
- `from . import relative` — relative import within a package

## File Patterns

- `__init__.py` — package initializer (barrel equivalent), can re-export public API
- `__main__.py` — package entry point when run with `python -m package`
- `conftest.py` — pytest shared fixtures and hooks (auto-discovered)
- `setup.py` / `pyproject.toml` — project configuration and build metadata
- `requirements.txt` — pinned dependency list

## Common Frameworks

- **Django** — Full-stack web framework with ORM, admin, and batteries included
- **FastAPI** — Modern async API framework with automatic OpenAPI docs
- **Flask** — Lightweight WSGI micro-framework for web applications
- **SQLAlchemy** — SQL toolkit and ORM with unit-of-work pattern
- **Celery** — Distributed task queue for background job processing
- **Pydantic** — Data validation and settings management using type annotations

## Example Language Notes

> Uses `@dataclass` decorator to auto-generate `__init__`, `__repr__`, and `__eq__` from
> field annotations. This eliminates boilerplate while keeping the class definition
> readable and the generated methods consistent.
>
> When `__init__.py` re-exports symbols, it acts as the package's public API surface —
> consumers import from the package rather than reaching into internal modules.

### shell
# Shell Language Prompt Snippet

## Key Concepts

- **Shebang Line**: `#!/bin/bash` or `#!/usr/bin/env bash` specifying the interpreter
- **Variables**: `VAR=value` assignment, `$VAR` or `${VAR}` expansion, no spaces around `=`
- **Functions**: `function name()` or `name()` for reusable command groups
- **Conditionals**: `if [[ condition ]]; then ... fi` with `[[ ]]` for extended tests
- **Loops**: `for item in list`, `while condition`, `until condition` iteration patterns
- **Pipes and Redirection**: `|` for chaining commands, `>` / `>>` / `2>&1` for output redirection
- **Exit Codes**: `$?` captures last command status; `set -e` exits on any failure
- **Strict Mode**: `set -euo pipefail` for robust error handling (exit on error, undefined vars, pipe failures)
- **Command Substitution**: `$(command)` captures command output as a string
- **Here Documents**: `<<EOF ... EOF` for multi-line string input to commands

## Notable File Patterns

- `*.sh` / `*.bash` — Shell script files
- `scripts/*.sh` — Project automation scripts (build, deploy, setup)
- `entrypoint.sh` — Docker container entry point script
- `install.sh` / `setup.sh` — Environment setup scripts
- `.bashrc` / `.bash_profile` / `.zshrc` — Shell configuration files

## Edge Patterns

- Shell scripts `triggers` other scripts or build processes they invoke
- Entry point scripts `deploys` the application they start
- Setup scripts `configures` the development environment
- Build scripts `depends_on` the source files they compile or package

## Summary Style

> "Build automation script compiling TypeScript, running tests, and packaging the release artifact."
> "Docker entry point script handling signal forwarding and graceful shutdown."
> "Environment setup script installing dependencies and configuring development tools."

### sql
# SQL Language Prompt Snippet

## Key Concepts

- **DDL (Data Definition)**: `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE` for schema management
- **DML (Data Manipulation)**: `SELECT`, `INSERT`, `UPDATE`, `DELETE` for data operations
- **Normalization**: Organizing tables to reduce redundancy through 1NF, 2NF, 3NF relationships
- **Foreign Keys**: `REFERENCES` constraints enforcing referential integrity between tables
- **Indexes**: `CREATE INDEX` for query performance optimization on frequently queried columns
- **Migrations**: Numbered, sequential schema changes applied in order for version control
- **Transactions**: `BEGIN`/`COMMIT`/`ROLLBACK` for atomic multi-statement operations
- **Views**: Named queries (`CREATE VIEW`) providing virtual tables for complex joins
- **Stored Procedures**: Server-side functions for encapsulating business logic in the database
- **Constraints**: `NOT NULL`, `UNIQUE`, `CHECK`, `DEFAULT` for data integrity rules

## Notable File Patterns

- `migrations/*.sql` — Numbered migration files (e.g., `001_create_users.sql`, `002_add_orders.sql`)
- `schema.sql` — Full database schema definition (often generated from migrations)
- `seeds/*.sql` — Seed data for development and testing environments
- `*.up.sql` / `*.down.sql` — Reversible migration pairs (up applies, down reverts)
- `init.sql` — Database initialization script for Docker or fresh setup
- `procedures/*.sql` — Stored procedure definitions

## Edge Patterns

- SQL migration files `migrates` the tables they create or alter
- Schema definition files `defines_schema` for the ORM models or data layer code that reads them
- Table definitions create implicit `related` edges between tables connected by foreign keys
- Seed files `depends_on` the migration files that create the tables they populate

## Summary Style

> "Database migration creating the users table with email, name, and authentication columns."
> "Schema definition with N tables covering user management, orders, and payment processing."
> "Seed data populating N tables with development fixtures for testing."

### yaml
# YAML Language Prompt Snippet

## Key Concepts

- **Indentation-Based Nesting**: Whitespace-sensitive structure (spaces only, no tabs) defining hierarchy
- **Anchors and Aliases**: `&anchor` defines a reusable block, `*anchor` references it to avoid duplication
- **Merge Keys**: `<<: *anchor` merges anchor contents into the current mapping
- **Multi-Line Strings**: Literal block (`|`) preserves newlines, folded block (`>`) joins lines
- **Document Separators**: `---` starts a new document, `...` ends one (multi-document streams)
- **Tags and Types**: `!!str`, `!!int`, `!!bool` for explicit typing; custom tags for application-specific types
- **Flow Style**: Inline JSON-like syntax `{key: value}` and `[item1, item2]` for compact notation
- **Environment Variable Substitution**: `${VAR}` patterns used in docker-compose and CI configs

## Notable File Patterns

- `docker-compose.yml` / `docker-compose.yaml` — Multi-container Docker application definition
- `.github/workflows/*.yml` — GitHub Actions CI/CD workflow definitions
- `.gitlab-ci.yml` — GitLab CI/CD pipeline configuration
- `kubernetes/*.yaml` / `k8s/*.yaml` — Kubernetes resource manifests
- `*.config.yaml` — Application configuration files
- `mkdocs.yml` — MkDocs documentation site configuration
- `serverless.yml` — Serverless Framework configuration

## Edge Patterns

- YAML config files `configures` the code modules they control (e.g., database settings affect data layer)
- CI/CD YAML files `triggers` build and deployment pipelines
- docker-compose YAML `deploys` services and `depends_on` Dockerfiles
- Kubernetes YAML `deploys` and `provisions` application services

## Summary Style

> "Docker Compose configuration defining N services with networking, volumes, and health checks."
> "GitHub Actions workflow running tests on push and deploying to production on merge to main."
> "Kubernetes deployment manifest with N replicas, resource limits, and liveness probes."


## Framework Addendum (FastAPI)
# FastAPI Framework Addendum

> Injected into file-analyzer and architecture-analyzer prompts when FastAPI is detected.
> Do NOT use as a standalone prompt — always appended to the base prompt template.

## FastAPI Project Structure

When analyzing a FastAPI project, apply these additional conventions on top of the base analysis rules.

### Canonical File Roles

| File / Pattern | Role | Tags |
|---|---|---|
| `main.py`, `app.py` | Application factory — creates and configures the `FastAPI()` instance | `entry-point`, `config` |
| `*/routers/*.py`, `*/api/*.py` | `APIRouter` modules — group related endpoints by domain | `api-handler`, `routing` |
| `*/schemas.py`, `*/schemas/*.py` | Pydantic request/response models | `type-definition`, `serialization` |
| `*/models.py`, `*/models/*.py` | SQLAlchemy ORM models or other DB models | `data-model` |
| `*/dependencies.py`, `*/deps.py` | `Depends()` provider functions — shared logic injected into routes | `service`, `middleware` |
| `*/crud.py`, `*/repository.py` | Database access layer — CRUD operations | `data-model`, `service` |
| `*/database.py`, `*/db.py` | DB engine, session factory, connection management | `config`, `data-model` |
| `*/config.py`, `*/settings.py` | `pydantic-settings` / `BaseSettings` config classes | `config` |
| `*/middleware.py` | Starlette middleware classes | `middleware` |
| `*/exceptions.py` | Custom exception classes and exception handlers | `utility` |
| `*/security.py`, `*/auth.py` | Auth utilities — JWT decoding, password hashing, OAuth helpers | `service`, `middleware` |
| `*/tasks.py` | Background tasks or Celery task definitions | `service`, `event-handler` |
| `*/tests/*.py`, `test_*.py` | pytest test files | `test` |
| `conftest.py` | pytest fixtures and test configuration | `test`, `config` |

### Edge Patterns to Look For

**Router inclusion chain** — When `app.include_router(some_router, prefix="/api")` appears in `main.py` or a router aggregator, create `imports` + `depends_on` edges from the main app file to each router module. This builds the URL hierarchy graph.

**Dependency injection tree** — When a route function or another `Depends()` provider imports and calls `Depends(some_function)`, create `depends_on` edges from the caller to the dependency provider. Trace these chains — they often span multiple files (e.g., route → auth dependency → DB session dependency).

**Pydantic model inheritance** — When a schema class inherits from another (e.g., `class UserCreate(UserBase)`), create `inherits` edges between the schema class nodes.

**ORM model relationships** — When SQLAlchemy models use `relationship()`, `ForeignKey`, create `depends_on` edges between the model classes.

**CRUD-to-model binding** — When a `crud.py` function takes a model type as an argument or directly references a model class, create `depends_on` edges from the CRUD file to the model file.

### Architectural Layers for FastAPI

| Layer ID | Layer Name | What Goes Here |
|---|---|---|
| `layer:api` | API Layer | Router files, endpoint functions with `@router.get/post/...` decorators |
| `layer:types` | Types Layer | Pydantic schema files, request/response models |
| `layer:service` | Service Layer | `dependencies.py`, `crud.py`, business logic modules |
| `layer:data` | Data Layer | ORM models, `database.py`, migrations |
| `layer:config` | Config Layer | `main.py` / `app.py` factory, `settings.py`, `config.py` |
| `layer:middleware` | Middleware Layer | `middleware.py`, `security.py`, `auth.py`, exception handlers |
| `layer:test` | Test Layer | `tests/`, `conftest.py` |

### Notable Patterns to Capture in languageLesson

- **Dependency injection as composition**: FastAPI's `Depends()` is a first-class DI system — a route can declare any number of dependencies, each of which can have their own dependencies, forming a tree resolved at request time
- **Pydantic for validation**: Request bodies, query params, and path params are automatically validated by Pydantic — invalid input raises `422 Unprocessable Entity` before your code runs
- **Async endpoints**: `async def` routes run in the event loop; `def` routes run in a threadpool — mixing them incorrectly can cause performance issues
- **Path operation order**: FastAPI matches routes in declaration order; a catch-all route before a specific one will shadow it


**Additional context from main session:**

Frameworks detected: APScheduler, Anthropic, AsyncPG, Discord.py, Docker, Docker Compose, FastAPI, OpenAI, Prometheus, Pydantic, Redis, Uvicorn

Directory tree (top 2 levels):
```
.claude/architecture.md
.claude/settings.json
.env.example
.gitattributes
.ua/config.json
CLAUDE.md
Makefile
README.md
backups/.gitkeep
docker-compose.yml
docker/api.Dockerfile
docker/bot.Dockerfile
docker/freetime_researcher.Dockerfile
docker/jupyter.Dockerfile
docker/market_data_logger.Dockerfile
docker/validation.Dockerfile
docker/watchdog.Dockerfile
docker/worker.Dockerfile
docs/BACKTEST_PLAYGROUND.md
docs/OBSERVABILITY.md
docs/RETROSPECTIVE_FROM_SCRATCH.md
graphify-out/.gitkeep
migrations/000_create_miniflux_db.sql
migrations/001_initial.sql
migrations/002_level2.sql
migrations/003_level3.sql
migrations/004_level4_validation.sql
migrations/005_signal_truth_layer.sql
migrations/006_replay_system.sql
migrations/007_observability.sql
migrations/008_governance.sql
migrations/009_meta_intelligence.sql
migrations/010_provider_ingestion_log.sql
migrations/011_signal_trace.sql
migrations/012_signal_feedback.sql
migrations/013_signal_edge_analyzer.sql
migrations/014_seed_tickers.sql
migrations/015_ticker_quotes.sql
migrations/016_paper_trades.sql
migrations/017_narrative_persistence.sql
migrations/018_technical_score.sql
migrations/019_signal_edge_agg_uq.sql
migrations/020_optimization_runs.sql
migrations/021_governance_log.sql
migrations/022_stale_quote_alert.sql
migrations/023_funding_liquidations.sql
migrations/024_research_runs.sql
migrations/025_infra_watchdog_alerts.sql
migrations/026_signal_trace_ticker.sql
migrations/028_retired_strategies.sql
migrations/029_personal_finance.sql
migrations/030_daily_briefs.sql
migrations/031_open_position_unique.sql
migrations/032_open_position_unique_per_mode.sql
migrations/033_news_impact_scoring.sql
migrations/034_truth_router.sql
migrations/035_trade_reflections.sql
migrations/036_etf_tickers.sql
migrations/037_fix_outcome_direction_correct.sql
migrations/038_equity_snapshots.sql
migrations/039_cide_decisions.sql
migrations/040_trade_post_mortems.sql
migrations/041_signal_trace_regime_filter.sql
migrations/042_capital_growth_log.sql
migrations/043_meta_strategy_runs.sql
migrations/044_paper_trades_signal_id_unique.sql
migrations/045_paper_trades_stop_order_ids.sql
migrations/046_funding_payments.sql
notebooks/.gitkeep
paper_trading_engine/Dockerfile
paper_trading_engine/adaptive_sizer.py
paper_trading_engine/broker_factory.py
paper_trading_engine/capital_growth.py
paper_trading_engine/capital_injection_engine.py
paper_trading_engine/discord_notifier.py
paper_trading_engine/discord_status.py
paper_trading_engine/engine.py
paper_trading_engine/equity_tracker.py
paper_trading_engine/executor.py
paper_trading_engine/gpt_validator.py
paper_trading_engine/llm_auditor.py
paper_trading_engine/main.py
paper_trading_engine/meta_strategy.py
paper_trading_engine/models.py
paper_trading_engine/notifier.py
paper_trading_engine/oos_validator.py
paper_trading_engine/portfolio.py
paper_trading_engine/requirements.txt
paper_trading_engine/risk.py
paper_trading_engine/risk_governor.py
paper_trading_engine/risk_guard.py
paper_trading_engine/truth_router.py
requirements-dev.txt
requirements.txt
scripts/aggregate_stats.py
scripts/apply_optimization.py
scripts/backfill_news.py
scripts/backtest.py
scripts/binance_income.py
scripts/check_sql_schema.py
scripts/cluster_analytics.py
scripts/critical_watchdog.py
scripts/deploy.sh
scripts/embed_news_qdrant.py
scripts/governance_engine.py
scripts/health_check.sh
scripts/ingest_binance_history.py
scripts/install_autopush.sh
scripts/lessons_learned.py
scripts/live_readiness.py
scripts/meanrev_replay.py
scripts/meanrev_seed_router.py
scripts/nightly_claude_diagnose.sh
scripts/oos_reeval.py
scripts/param_sweep.py
scripts/pipeline_report.py
scripts/pipeline_smoke_test.py
scripts/preflight.sh
scripts/replay_signals.py
scripts/router_walkforward.py
scripts/run_lessons_pipeline.py
scripts/show_effective_config.py
scripts/sql_check_allow.txt
signal_edge_analyzer/Dockerfile
signal_edge_analyzer/aggregator.py
signal_edge_analyzer/collector.py
signal_edge_analyzer/db.py
signal_edge_analyzer/discord_client.py
signal_edge_analyzer/main.py
signal_edge_analyzer/metrics.py
signal_edge_analyzer/models.py
signal_edge_analyzer/outcomes.py
signal_edge_analyzer/requirements.txt
signal_edge_analyzer/scheduler.py
signal_edge_analyzer/walkforward.py
sql/create_analyst_role.sql
src/__init__.py
tests/conftest.py
tests/test_autopsy.py
tests/test_binance_futures.py
tests/test_dashboard_proxy.py
tests/test_executor.py
tests/test_exit_priority.py
tests/test_graveyard.py
tests/test_portfolio.py
tests/test_reconcile_ignore.py
tests/test_risk.py
tests/test_stale_flatten.py
tests/test_truth_router.py
```
Top-level file counts: {".claude":23,"(root)":8,".ua":1,"backups":1,"docker":8,"docs":3,"graphify-out":1,"migrations":46,"monitoring":11,"notebooks":1,"paper_trading_engine":29,"scripts":31,"signal_edge_analyzer":12,"sql":6,"src":149,"tests":12}

Use the directory tree, language context, and framework addendum to inform layer assignments. Directory structure is strong evidence for layer boundaries. Non-code files (config, docs, infrastructure, data) should be assigned to appropriate layers.

> **Language directive**: Generate all textual content (summaries, descriptions, tags, titles, languageNotes, languageLesson) in **English**. Maintain technical accuracy while using natural, native-level phrasing in the target language. Keep technical terms in English when no standard translation exists (e.g., "middleware", "hook", "barrel").
