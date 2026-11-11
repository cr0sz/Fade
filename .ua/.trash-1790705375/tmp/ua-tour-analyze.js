#!/usr/bin/env node
"use strict";
const fs = require("fs");

function fail(msg) {
  console.error(msg);
  process.exit(1);
}

const [, , inPath, outPath] = process.argv;
if (!inPath || !outPath) fail("Usage: node ua-tour-analyze.js <input.json> <output.json>");

let data;
try {
  data = JSON.parse(fs.readFileSync(inPath, "utf8"));
} catch (e) {
  fail("Failed to read input: " + e.message);
}

const nodes = data.nodes || [];
const edges = data.edges || [];
const layers = data.layers || [];
const nodeById = new Map(nodes.map((n) => [n.id, n]));

const fanIn = new Map();
const fanOut = new Map();
const fwd = new Map(); // imports/calls forward adjacency
const inSet = new Map();
const outSet = new Map();
const edgeTypeCounts = {};
nodes.forEach((n) => {
  fanIn.set(n.id, 0);
  fanOut.set(n.id, 0);
  inSet.set(n.id, new Set());
  outSet.set(n.id, new Set());
});

for (const e of edges) {
  edgeTypeCounts[e.type] = (edgeTypeCounts[e.type] || 0) + 1;
  if (!nodeById.has(e.source) || !nodeById.has(e.target)) continue;
  if (e.source === e.target) continue;
  if (!outSet.get(e.source).has(e.target)) {
    outSet.get(e.source).add(e.target);
    fanOut.set(e.source, fanOut.get(e.source) + 1);
  }
  if (!inSet.get(e.target).has(e.source)) {
    inSet.get(e.target).add(e.source);
    fanIn.set(e.target, fanIn.get(e.target) + 1);
  }
  if (e.type === "imports" || e.type === "calls") {
    if (!fwd.has(e.source)) fwd.set(e.source, []);
    fwd.get(e.source).push(e.target);
  }
}

const rank = (m, key) =>
  [...m.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, 20)
    .map(([id, v]) => ({ id, [key]: v, name: nodeById.get(id).name }));

const fanInRanking = rank(fanIn, "fanIn");
const fanOutRanking = rank(fanOut, "fanOut");

// Entry points
const entryNames = new Set([
  "index.ts", "index.js", "main.ts", "main.js", "app.ts", "app.js", "server.ts", "server.js",
  "mod.rs", "main.go", "main.py", "main.rs", "manage.py", "app.py", "wsgi.py", "asgi.py",
  "run.py", "__main__.py", "Application.java", "Main.java", "Program.cs", "config.ru",
  "index.php", "App.swift", "Application.kt", "main.cpp", "main.c",
]);
const codeNodes = nodes.filter((n) => n.type === "file");
const sortedOut = [...codeNodes].map((n) => fanOut.get(n.id)).sort((a, b) => b - a);
const sortedIn = [...codeNodes].map((n) => fanIn.get(n.id)).sort((a, b) => a - b);
const outThreshold = sortedOut[Math.max(0, Math.floor(sortedOut.length * 0.1) - 1)] || 0;
const inThreshold = sortedIn[Math.max(0, Math.floor(sortedIn.length * 0.25) - 1)] || 0;

const candidates = [];
for (const n of nodes) {
  const fp = (n.filePath || "").replace(/\\/g, "/");
  const base = fp.split("/").pop();
  const depth = fp.split("/").length - 1;
  let score = 0;
  if (n.type === "file") {
    if (entryNames.has(base)) score += 3;
    if (depth <= 1) score += 1;
    if (fanOut.get(n.id) >= outThreshold && fanOut.get(n.id) > 0) score += 1;
    if (fanIn.get(n.id) <= inThreshold) score += 1;
  } else if (n.type === "document") {
    if (fp === "README.md") score += 5;
    else if (/\.md$/i.test(fp) && depth === 0) score += 2;
  }
  if (score > 0) candidates.push({ id: n.id, score, name: n.name, summary: n.summary });
}
candidates.sort((a, b) => b.score - a.score || a.id.localeCompare(b.id));
const entryPointCandidates = candidates.slice(0, 5);

// Also report the runtime entry points explicitly named by the dispatch prompt if present
const namedEntries = ["src/api/main.py", "src/scheduler/worker.py", "paper_trading_engine/main.py"]
  .map((p) => nodes.find((n) => (n.filePath || "").replace(/\\/g, "/") === p))
  .filter(Boolean)
  .map((n) => ({ id: n.id, name: n.name, fanIn: fanIn.get(n.id), fanOut: fanOut.get(n.id) }));

// BFS from top code entry (prefer one of the named runtime entry points if generic scoring finds none good)
function bfs(start) {
  const order = [];
  const depthMap = {};
  const byDepth = {};
  if (!start) return { startNode: null, order, depthMap, byDepth };
  const q = [start];
  depthMap[start] = 0;
  while (q.length) {
    const cur = q.shift();
    order.push(cur);
    const d = depthMap[cur];
    (byDepth[d] = byDepth[d] || []).push(cur);
    for (const nx of fwd.get(cur) || []) {
      if (!(nx in depthMap)) {
        depthMap[nx] = d + 1;
        q.push(nx);
      }
    }
  }
  return { startNode: start, order, depthMap, byDepth };
}
const topCode = candidates.find((c) => nodeById.get(c.id).type === "file");
const bfsTraversal = bfs(topCode ? topCode.id : null);
const bfsByNamedEntry = {};
for (const ne of namedEntries) {
  const r = bfs(ne.id);
  bfsByNamedEntry[ne.id] = { reached: r.order.length, byDepth: r.byDepth };
}

// Non-code inventory
const pick = (types) =>
  nodes
    .filter((n) => types.includes(n.type))
    .map((n) => ({ id: n.id, name: n.name, type: n.type, summary: n.summary }));
const nonCodeFiles = {
  documentation: pick(["document"]),
  infrastructure: pick(["service", "pipeline", "resource"]),
  data: pick(["table", "schema", "endpoint"]),
  config: pick(["config"]),
};

// Clusters: bidirectional pairs on imports/calls, expand
const fwdSet = new Map();
for (const [s, ts] of fwd) fwdSet.set(s, new Set(ts));
const seen = new Set();
const clusters = [];
for (const [s, ts] of fwdSet) {
  for (const t of ts) {
    if (fwdSet.get(t) && fwdSet.get(t).has(s) && s < t) {
      const key = s + "|" + t;
      if (seen.has(key)) continue;
      seen.add(key);
      const cl = new Set([s, t]);
      // expand
      let changed = true;
      while (changed && cl.size < 5) {
        changed = false;
        const cand = new Map();
        for (const m of cl) {
          const nb = new Set([...(fwdSet.get(m) || []), ...[...fwdSet.keys()].filter((k) => fwdSet.get(k).has(m))]);
          for (const x of nb) if (!cl.has(x)) cand.set(x, (cand.get(x) || 0) + 1);
        }
        for (const [x, c] of cand) {
          if (c >= 2 && cl.size < 5) {
            cl.add(x);
            changed = true;
          }
        }
      }
      const arr = [...cl];
      let ec = 0;
      for (const a of arr) for (const b of arr) if (a !== b && fwdSet.get(a) && fwdSet.get(a).has(b)) ec++;
      clusters.push({ nodes: arr, edgeCount: ec });
    }
  }
}
clusters.sort((a, b) => b.edgeCount - a.edgeCount);
const dedup = [];
const usedKeys = new Set();
for (const c of clusters) {
  const k = [...c.nodes].sort().join("|");
  if (usedKeys.has(k)) continue;
  usedKeys.add(k);
  dedup.push(c);
}

const nodeSummaryIndex = {};
for (const n of nodes) nodeSummaryIndex[n.id] = { name: n.name, type: n.type, summary: n.summary };

const nodeTypeCounts = {};
nodes.forEach((n) => (nodeTypeCounts[n.type] = (nodeTypeCounts[n.type] || 0) + 1));

const results = {
  scriptCompleted: true,
  entryPointCandidates,
  namedRuntimeEntries: namedEntries,
  fanInRanking,
  fanOutRanking,
  bfsTraversal,
  bfsByNamedEntry,
  nonCodeFiles,
  clusters: dedup.slice(0, 10),
  layers: { count: layers.length, list: layers.map((l) => ({ id: l.id, name: l.name, description: l.description })) },
  nodeSummaryIndex,
  edgeTypeCounts,
  nodeTypeCounts,
  totalNodes: nodes.length,
  totalEdges: edges.length,
};
fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
process.exit(0);
