"""End-to-end smoke test for the LLM lessons/governance pipeline.

Runs the full chain (aggregate → 6-pass LLM → governance) with governance in
dry-run mode, printing stage timings. Use after infra changes to verify the
pipeline actually works — it silently failed for weeks when Ollama was
unreachable from containers (fixed in Sprint 35).

Usage (inside signal-edge-analyzer container):
    python3 /app/scripts/pipeline_smoke_test.py
"""
import sys, asyncio, json, tempfile, os, time

sys.path.insert(0, '/app/scripts')
sys.path.insert(0, '/app')

t0 = time.time()
from aggregate_stats import aggregate
from lessons_learned import run as run_lessons
from governance_engine import run as run_governance


async def main():
    stats = await aggregate(60)
    print(f"[{time.time()-t0:.0f}s] stats aggregated: "
          f"{stats.get('summary', {}).get('count')} trades", flush=True)
    with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False) as sf:
        json.dump(stats, sf, default=str)
        sp = sf.name
    lp = tempfile.mktemp(suffix='.json')
    run_lessons(sp, lp, os.getenv('OLLAMA_MODEL', 'qwen2.5:7b'),
                os.getenv('OLLAMA_BASE_URL', 'http://ollama:11434'),
                fast_model=os.getenv('OLLAMA_FAST_MODEL', 'qwen2.5:3b'),
                use_fast=True)
    print(f"[{time.time()-t0:.0f}s] lessons complete", flush=True)
    out = json.load(open(lp))
    print("lessons keys:", list(out)[:12], flush=True)
    await run_governance(lp, dry_run=True)
    print(f"[{time.time()-t0:.0f}s] governance dry-run complete", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
