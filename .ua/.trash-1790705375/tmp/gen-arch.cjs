const fs=require('fs');
const root='C:/Users/bekir/Documents/projeler/Fade', ua=root+'/.ua';
const S='C:/Users/bekir/.claude/plugins/cache/understand-anything/understand-anything/2.9.7/skills/understand';
const g=JSON.parse(fs.readFileSync(ua+'/intermediate/assembled-graph.json','utf8'));
const scan=JSON.parse(fs.readFileSync(ua+'/intermediate/scan-result.json','utf8'));
const ft=new Set(['file','config','document','service','pipeline','table','schema','resource','endpoint']);
const fileNodes=g.nodes.filter(n=>ft.has(n.type)).map(n=>({id:n.id,type:n.type,name:n.name,filePath:n.filePath,summary:n.summary,tags:n.tags}));
const ids=new Set(fileNodes.map(n=>n.id));
const importEdges=g.edges.filter(e=>e.type==='imports');
fs.writeFileSync(ua+'/tmp/arch-input.json',JSON.stringify({fileNodes,importEdges,allEdges:g.edges.filter(e=>ids.has(e.source)&&ids.has(e.target)&&e.type!=='contains')}));
let lang='';
for(const l of ['css','dockerfile','html','javascript','json','markdown','python','shell','sql','yaml']){const p=`${S}/languages/${l}.md`; if(fs.existsSync(p)) lang+=`\n### ${l}\n`+fs.readFileSync(p,'utf8');}
const fw=fs.readFileSync(`${S}/frameworks/fastapi.md`,'utf8');
const tree=scan.files.filter(f=>f.path.split('/').length<=2).map(f=>f.path).join('\n');
const dirs={};scan.files.forEach(f=>{const d=f.path.includes('/')?f.path.split('/')[0]:'(root)';dirs[d]=(dirs[d]||0)+1});
const prompt=`Analyze this codebase's structure to identify architectural layers.
Project root: \`${root}\`
Write output to: \`${ua}/intermediate/layers.json\` (plain JSON array of layers: {id, name, description, nodeIds})
Project: \`${scan.name}\` — \`${scan.description}\`

Input data (READ this file; it contains the three JSON blocks the agent definition expects):
\`${ua}/tmp/arch-input.json\` = { "fileNodes": [{id,type,name,filePath,summary,tags} for every file-level node — ${fileNodes.length} nodes], "importEdges": [edges of type imports], "allEdges": [all non-contains edges between file-level nodes] }.
Every file-level node id in fileNodes MUST appear in exactly one layer's nodeIds. Use node ids exactly as given.

## Language Context
${lang}

## Framework Addendum (FastAPI)
${fw}

**Additional context from main session:**

Frameworks detected: ${scan.frameworks.join(', ')}

Directory tree (top 2 levels):
\`\`\`
${tree}
\`\`\`
Top-level file counts: ${JSON.stringify(dirs)}

Use the directory tree, language context, and framework addendum to inform layer assignments. Directory structure is strong evidence for layer boundaries. Non-code files (config, docs, infrastructure, data) should be assigned to appropriate layers.

> **Language directive**: Generate all textual content (summaries, descriptions, tags, titles, languageNotes, languageLesson) in **English**. Maintain technical accuracy while using natural, native-level phrasing in the target language. Keep technical terms in English when no standard translation exists (e.g., "middleware", "hook", "barrel").
`;
fs.writeFileSync(ua+'/tmp/prompt-architecture.md',prompt);
console.log('fileNodes',fileNodes.length,'importEdges',importEdges.length,'prompt bytes',prompt.length);
