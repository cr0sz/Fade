const fs = require('fs');
const path = require('path');

const [, , inPath, outPath] = process.argv;
if (!inPath || !outPath) { console.error('usage: node script in out'); process.exit(1); }
let data;
try { data = JSON.parse(fs.readFileSync(inPath, 'utf8')); } catch (e) { console.error(String(e)); process.exit(1); }

const nodes = data.fileNodes || [];
const importEdges = data.importEdges || [];
const allEdges = data.allEdges || [];
const byId = new Map(nodes.map(n => [n.id, n]));

// A. directory grouping
const paths = nodes.map(n => (n.filePath || '').replace(/\\/g, '/'));
function commonPrefix(ps) {
  if (!ps.length) return '';
  const segs = ps.map(p => p.split('/').slice(0, -1));
  let pre = segs[0];
  for (const s of segs) {
    let i = 0;
    while (i < pre.length && i < s.length && pre[i] === s[i]) i++;
    pre = pre.slice(0, i);
  }
  return pre.length ? pre.join('/') + '/' : '';
}
const prefix = commonPrefix(paths);
function groupOf(n) {
  const p = (n.filePath || '').replace(/\\/g, '/');
  const rel = p.startsWith(prefix) ? p.slice(prefix.length) : p;
  const parts = rel.split('/');
  if (parts.length === 1) return '(root)';
  return parts[0];
}
const directoryGroups = {};
const groupById = {};
for (const n of nodes) {
  const g = groupOf(n);
  groupById[n.id] = g;
  (directoryGroups[g] = directoryGroups[g] || []).push(n.id);
}

// B. node type grouping
const nodeTypeGroups = {};
for (const n of nodes) (nodeTypeGroups[n.type] = nodeTypeGroups[n.type] || []).push(n.id);

// C. adjacency
const fileFanIn = {}, fileFanOut = {};
const groupOut = {}, groupIn = {};
for (const e of importEdges) {
  if (!byId.has(e.source) || !byId.has(e.target)) continue;
  fileFanOut[e.source] = (fileFanOut[e.source] || 0) + 1;
  fileFanIn[e.target] = (fileFanIn[e.target] || 0) + 1;
  const a = groupById[e.source], b = groupById[e.target];
  (groupOut[a] = groupOut[a] || new Set()).add(b);
  (groupIn[b] = groupIn[b] || new Set()).add(a);
}

// D. cross-category
const cc = {};
for (const e of allEdges) {
  const s = byId.get(e.source), t = byId.get(e.target);
  if (!s || !t) continue;
  const k = `${s.type}|${t.type}|${e.type}`;
  cc[k] = (cc[k] || 0) + 1;
}
const crossCategoryEdges = Object.entries(cc).map(([k, count]) => {
  const [fromType, toType, edgeType] = k.split('|');
  return { fromType, toType, edgeType, count };
}).sort((a, b) => b.count - a.count);

// E. inter-group
const ig = {};
for (const e of importEdges) {
  if (!byId.has(e.source) || !byId.has(e.target)) continue;
  const a = groupById[e.source], b = groupById[e.target];
  if (a === b) continue;
  const k = `${a}|${b}`;
  ig[k] = (ig[k] || 0) + 1;
}
const interGroupImports = Object.entries(ig).map(([k, count]) => {
  const [from, to] = k.split('|');
  return { from, to, count };
}).sort((a, b) => b.count - a.count);

// F. intra-group density
const intraGroupDensity = {};
for (const g of Object.keys(directoryGroups)) intraGroupDensity[g] = { internalEdges: 0, totalEdges: 0, density: 0 };
for (const e of importEdges) {
  if (!byId.has(e.source) || !byId.has(e.target)) continue;
  const a = groupById[e.source], b = groupById[e.target];
  if (a === b) { intraGroupDensity[a].internalEdges++; intraGroupDensity[a].totalEdges++; }
  else { intraGroupDensity[a].totalEdges++; intraGroupDensity[b].totalEdges++; }
}
for (const g of Object.values(intraGroupDensity)) g.density = g.totalEdges ? +(g.internalEdges / g.totalEdges).toFixed(2) : 0;

// G. pattern matching
const dirPatterns = {
  api: ['routes', 'api', 'controllers', 'endpoints', 'handlers', 'serializers', 'controller', 'routers', 'blueprints'],
  service: ['services', 'core', 'lib', 'domain', 'logic', 'signals', 'internal', 'composables', 'mailers', 'jobs', 'channels'],
  data: ['models', 'db', 'data', 'persistence', 'repository', 'entities', 'migrations', 'entity', 'sql', 'database', 'schema'],
  ui: ['components', 'views', 'pages', 'ui', 'layouts', 'screens'],
  middleware: ['middleware', 'plugins', 'interceptors', 'guards'],
  utility: ['utils', 'helpers', 'common', 'shared', 'tools', 'templatetags', 'pkg'],
  config: ['config', 'constants', 'env', 'settings', 'management', 'commands'],
  test: ['__tests__', 'test', 'tests', 'spec', 'specs'],
  types: ['types', 'interfaces', 'schemas', 'contracts', 'dtos', 'dto', 'request', 'response'],
  hooks: ['hooks'],
  state: ['store', 'state', 'reducers', 'actions', 'slices'],
  assets: ['assets', 'static', 'public'],
  entry: ['cmd', 'bin'],
  documentation: ['docs', 'documentation', 'wiki'],
  infrastructure: ['deploy', 'deployment', 'infra', 'infrastructure', 'k8s', 'kubernetes', 'helm', 'charts', 'terraform', 'tf', 'docker'],
  'ci-cd': ['.github', '.gitlab', '.circleci'],
};
const patternMatches = {};
for (const g of Object.keys(directoryGroups)) {
  const lg = g.toLowerCase();
  for (const [label, names] of Object.entries(dirPatterns)) {
    if (names.includes(lg)) { patternMatches[g] = label; break; }
  }
}
function filePattern(p) {
  const base = path.posix.basename(p);
  if (/\.(test|spec)\./.test(base) || /^test_.*\.py$/.test(base) || /_test\.go$/.test(base) || /Tests?\.(java|php|cs)$/.test(base) || /_spec\.rb$/.test(base)) return 'test';
  if (/\.d\.ts$/.test(base)) return 'types';
  if (['index.ts', 'index.js', '__init__.py'].includes(base)) return 'entry';
  if (base === 'manage.py') return 'entry';
  if (['wsgi.py', 'asgi.py'].includes(base)) return 'config';
  if (['Cargo.toml', 'go.mod', 'Gemfile', 'pom.xml', 'build.gradle', 'composer.json'].includes(base)) return 'config';
  if (/^Dockerfile/.test(base) || /^docker-compose\./.test(base)) return 'infrastructure';
  if (/\.(tf|tfvars)$/.test(base)) return 'infrastructure';
  if (/^\.github\/workflows\//.test(p) || base === '.gitlab-ci.yml' || base === 'Jenkinsfile') return 'ci-cd';
  if (/\.sql$/.test(base)) return 'data';
  if (/\.(graphql|gql|proto)$/.test(base)) return 'types';
  if (/\.(md|rst)$/.test(base)) return 'documentation';
  if (base === 'Makefile') return 'infrastructure';
  return null;
}
const filePatternMatches = {};
for (const n of nodes) {
  const fp = filePattern((n.filePath || '').replace(/\\/g, '/'));
  if (fp) filePatternMatches[n.id] = fp;
}

// H. deployment topology
const fps = nodes.map(n => (n.filePath || '').replace(/\\/g, '/'));
const infraFiles = fps.filter(p => /Dockerfile|docker-compose|\.tf$|\.tfvars$|\.github\/workflows|\.gitlab-ci|Jenkinsfile|Makefile|k8s|helm/i.test(p));
const deploymentTopology = {
  hasDockerfile: fps.some(p => /(^|\/)Dockerfile/.test(p)),
  hasCompose: fps.some(p => /docker-compose/.test(p)),
  hasK8s: fps.some(p => /(k8s|kubernetes|helm)/i.test(p)),
  hasTerraform: fps.some(p => /\.tf$/.test(p)),
  hasCI: fps.some(p => /\.github\/workflows|\.gitlab-ci|Jenkinsfile|\.circleci/.test(p)),
  infraFiles,
};

// I. data pipeline
const dataPipeline = {
  schemaFiles: fps.filter(p => /\.(graphql|gql|proto|prisma)$/.test(p) || /schema/i.test(path.posix.basename(p))),
  migrationFiles: fps.filter(p => /migrations?\//.test(p) && /\.sql$/.test(p)),
  dataModelFiles: fps.filter(p => /(^|\/)(models|entities|db)\//.test(p)),
  apiHandlerFiles: fps.filter(p => /(^|\/)(routes|api|controllers|handlers|routers)\//.test(p)),
};

// J. doc coverage
const docNodes = nodes.filter(n => n.type === 'document');
const groupsWithDocsSet = new Set();
for (const d of docNodes) {
  const p = (d.filePath || '').replace(/\\/g, '/');
  const dg = groupOf(d);
  if (dg !== '(root)' && (/readme/i.test(p) || dg === 'docs')) groupsWithDocsSet.add(dg);
}
for (const e of allEdges) {
  const s = byId.get(e.source), t = byId.get(e.target);
  if (s && t && s.type === 'document' && t.type !== 'document') groupsWithDocsSet.add(groupById[t.id]);
}
const realGroups = Object.keys(directoryGroups);
const docCoverage = {
  groupsWithDocs: groupsWithDocsSet.size,
  totalGroups: realGroups.length,
  coverageRatio: realGroups.length ? +(groupsWithDocsSet.size / realGroups.length).toFixed(2) : 0,
  undocumentedGroups: realGroups.filter(g => !groupsWithDocsSet.has(g)),
};

// K. dependency direction
const dependencyDirection = [];
const seen = new Set();
for (const { from, to, count } of interGroupImports) {
  const key = [from, to].sort().join('|');
  if (seen.has(key)) continue;
  seen.add(key);
  const back = ig[`${to}|${from}`] || 0;
  if (count >= back) dependencyDirection.push({ dependent: from, dependsOn: to, count, reverse: back });
  else dependencyDirection.push({ dependent: to, dependsOn: from, count: back, reverse: count });
}

const filesPerGroup = {};
for (const [g, ids] of Object.entries(directoryGroups)) filesPerGroup[g] = ids.length;
const nodeTypeCounts = {};
for (const [t, ids] of Object.entries(nodeTypeGroups)) nodeTypeCounts[t] = ids.length;

const result = {
  scriptCompleted: true,
  commonPrefix: prefix,
  directoryGroups,
  nodeTypeGroups,
  crossCategoryEdges,
  interGroupImports,
  intraGroupDensity,
  patternMatches,
  filePatternMatches,
  deploymentTopology,
  dataPipeline,
  docCoverage,
  dependencyDirection,
  groupImportAdjacency: Object.fromEntries(Object.keys(directoryGroups).map(g => [g, { imports: [...(groupOut[g] || [])], importedBy: [...(groupIn[g] || [])] }])),
  fileStats: { totalFileNodes: nodes.length, filesPerGroup, nodeTypeCounts },
  fileFanIn,
  fileFanOut,
};
fs.writeFileSync(outPath, JSON.stringify(result, null, 2));
process.exit(0);
