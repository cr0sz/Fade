const fs=require('fs');
const root='C:/Users/bekir/Documents/projeler/Fade';
const ua=root+'/.ua';
const skillDir='C:/Users/bekir/.claude/plugins/cache/understand-anything/understand-anything/2.9.7/skills/understand';
const b=JSON.parse(fs.readFileSync(ua+'/intermediate/batches.json','utf8'));
const scan=JSON.parse(fs.readFileSync(ua+'/intermediate/scan-result.json','utf8'));
const langs=scan.languages.join(', ');
for(const x of b.batches){
  const list=x.files.map((f,i)=>`${i+1}. \`${f.path}\` (${f.sizeLines} lines, language: \`${f.language}\`, fileCategory: \`${f.fileCategory}\`)`).join('\n');
  const p=`Analyze these files and produce GraphNode and GraphEdge objects.
Project root: \`${root}\`
Project: \`${scan.name}\` — \`${scan.description}\`
Languages: \`${langs}\`
Batch: \`${x.batchIndex}/${b.totalBatches}\`
Skill directory (for bundled scripts): \`${skillDir}\`
Data directory ($UA_DIR): \`${ua}\`
Output: write to \`${ua}/intermediate/batch-${x.batchIndex}.json\` (single-file mode) OR \`batch-${x.batchIndex}-part-<k>.json\` (split mode, per Step B of your output protocol).

Pre-resolved import data for this batch (use directly — do NOT re-resolve imports from source):
\`\`\`json
${JSON.stringify(x.batchImportData)}
\`\`\`

Cross-batch neighbors with their exported symbols (confidence boost for cross-batch edges):
\`\`\`json
${JSON.stringify(x.neighborMap)}
\`\`\`

Files to analyze in this batch (every entry MUST be passed through to \`batchFiles\` with all four fields — \`path\`, \`language\`, \`sizeLines\`, \`fileCategory\`):
${list}

**Additional context from main session:**

Project: \`${scan.name}\` — \`${scan.description}\`
Languages: \`${langs}\`

> **Language directive**: Generate all textual content (summaries, descriptions, tags, titles, languageNotes, languageLesson) in **English**. Maintain technical accuracy while using natural, native-level phrasing in the target language. Keep technical terms in English when no standard translation exists (e.g., "middleware", "hook", "barrel").

Never read \`.env\` or any secrets file; it is not in your batch.
`;
  fs.writeFileSync(`${ua}/tmp/prompt-batch-${x.batchIndex}.md`,p);
}
console.log('wrote',b.batches.length);
