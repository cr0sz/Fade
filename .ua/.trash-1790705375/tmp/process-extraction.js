const fs = require('fs');
const path = require('path');

const UA_DIR = 'C:/Users/bekir/Documents/projeler/Fade/.ua';
const batchIndex = 1;

// Read extraction results
const extractionFile = path.join(UA_DIR, 'tmp', `ua-file-extract-results-${batchIndex}.json`);
const extraction = JSON.parse(fs.readFileSync(extractionFile, 'utf8'));

// Read import data
const inputFile = path.join(UA_DIR, 'tmp', `ua-file-analyzer-input-${batchIndex}.json`);
const input = JSON.parse(fs.readFileSync(inputFile, 'utf8'));
const batchImportData = input.batchImportData;

const nodes = [];
const edges = [];
const addedNodeIds = new Set();

// Helper to add unique node
function addNode(node) {
  if (!addedNodeIds.has(node.id)) {
    nodes.push(node);
    addedNodeIds.add(node.id);
  }
}

// Helper to add edge
function addEdge(edge) {
  edges.push(edge);
}

// Determine file purpose tags and summary
function getFileSummaryAndTags(filePath) {
  const fileName = path.basename(filePath);
  let summary = '';
  let tags = ['python'];

  if (filePath.includes('broker') || filePath.includes('binance') || filePath.includes('alpaca') || filePath.includes('paper')) {
    tags = ['broker-adapter', 'trading', 'exchange-integration'];
    if (filePath.endsWith('base.py')) {
      summary = 'Abstract base class defining the broker interface for unified exchange connectivity.';
      tags.push('interface');
    } else if (filePath.includes('binance_futures')) {
      summary = 'Binance Futures broker implementation with leveraged trading and risk management for crypto derivatives.';
      tags = ['broker-adapter', 'futures-trading', 'exchange-integration'];
    } else if (filePath.includes('binance.py')) {
      summary = 'Binance spot exchange adapter providing order placement, execution, and account management.';
    } else if (filePath.includes('alpaca')) {
      summary = 'Alpaca equity broker adapter for US equities trading with real-time market data integration.';
      tags = ['broker-adapter', 'equity-trading', 'alpaca'];
    } else if (filePath.includes('paper.py')) {
      summary = 'Paper trading broker simulating fills and tracking portfolio without real capital.';
      tags = ['paper-trading', 'simulation', 'broker-adapter'];
    } else if (filePath.includes('__init__')) {
      summary = 'Broker module initialization exposing broker implementations and factory functions.';
      tags = ['entry-point', 'module-init'];
    }
  } else if (filePath === 'paper_trading_engine/engine.py') {
    summary = 'Core trading engine orchestrating signal reception, validation, portfolio management, and order execution with comprehensive risk controls.';
    tags = ['entry-point', 'orchestration', 'core-engine', 'trading-logic'];
  } else if (filePath.includes('broker_factory')) {
    summary = 'Factory pattern implementation for creating broker instances based on configuration.';
    tags = ['factory', 'broker-adapter', 'configuration'];
  } else if (filePath.includes('adaptive_sizer')) {
    summary = 'Position sizing calculator that adapts trade sizes based on asset performance, volatility, and win streaks.';
    tags = ['position-sizing', 'risk-management', 'strategy'];
  } else if (filePath.includes('portfolio')) {
    summary = 'Portfolio state management tracking positions, holdings, and order execution for active trades.';
    tags = ['state-management', 'trading-logic', 'data-model'];
  } else if (filePath.includes('executor')) {
    summary = 'Order execution engine handling trade submission, fill tracking, and order state transitions.';
    tags = ['order-execution', 'trading-logic', 'risk-management'];
  } else if (filePath.includes('models')) {
    summary = 'Shared data models for trades, positions, orders, and configuration using Pydantic validation.';
    tags = ['data-model', 'type-definition', 'validation'];
  } else if (filePath.includes('risk.py')) {
    summary = 'Risk calculation module computing position risk, portfolio exposure, and stop-loss levels.';
    tags = ['risk-management', 'calculations', 'metrics'];
  } else if (filePath.includes('risk_guard')) {
    summary = 'Hard safety layer blocking trades that violate position limits, drawdown, or leverage constraints.';
    tags = ['safety', 'risk-management', 'guardrail'];
  } else if (filePath.includes('risk_governor')) {
    summary = 'Regulatory-style risk governance applying account-level drawdown limits and position constraints.';
    tags = ['safety', 'risk-management', 'governance'];
  } else if (filePath.includes('oos_validator')) {
    summary = 'Out-of-sample validation engine comparing backtest expectations against live trade results to detect signal degradation.';
    tags = ['validation', 'signal-quality', 'live-monitoring'];
  } else if (filePath.includes('truth_router')) {
    summary = 'Signal routing engine that evaluates signal quality and routes high-confidence signals to paper or live trading.';
    tags = ['signal-routing', 'decision-logic', 'orchestration'];
  } else if (filePath.includes('gpt_validator')) {
    summary = 'LLM-based signal validator using GPT to assess trade rationale and market context for decision support.';
    tags = ['validation', 'llm-integration', 'ai-model'];
  } else if (filePath.includes('llm_auditor')) {
    summary = 'Comprehensive trading audit system using LLM to explain trade decisions, review outcomes, and generate performance insights.';
    tags = ['audit', 'llm-integration', 'analytics', 'ai-model'];
  } else if (filePath.includes('discord_notifier')) {
    summary = 'Discord notification service for trade alerts, signals, and portfolio updates using webhook integration.';
    tags = ['notifications', 'discord', 'integration'];
  } else if (filePath.includes('discord_status')) {
    summary = 'Discord status poster displaying real-time portfolio metrics, drawdown, and trading statistics.';
    tags = ['notifications', 'discord', 'monitoring'];
  } else if (filePath.includes('notifier')) {
    summary = 'Notification dispatcher routing alerts to configured channels (Discord, email, etc).';
    tags = ['notifications', 'integration'];
  } else if (filePath.includes('equity_tracker')) {
    summary = 'Equity curve tracker maintaining running portfolio value and performance metrics.';
    tags = ['monitoring', 'metrics', 'state-management'];
  } else if (filePath.includes('capital_growth')) {
    summary = 'Profit reinvestment module for pyramiding positions as capital grows from successful trades.';
    tags = ['position-management', 'capital-growth', 'strategy'];
  } else if (filePath.includes('cluster_analytics')) {
    summary = 'Analytics tool for clustering and analyzing signal outcomes to identify patterns in trading results.';
    tags = ['analytics', 'data-analysis', 'signal-analysis'];
  }

  return { summary, tags };
}

// Process each file from extraction results
extraction.results.forEach(result => {
  const filePath = result.path;
  const fileName = path.basename(filePath);
  const fileId = 'file:' + filePath;

  // Determine complexity
  let complexity = 'simple';
  if (result.nonEmptyLines > 200) {
    complexity = 'complex';
  } else if (result.nonEmptyLines > 50) {
    complexity = 'moderate';
  }

  // Get summary and tags
  const { summary, tags } = getFileSummaryAndTags(filePath);

  // Create file node
  const fileNode = {
    id: fileId,
    type: 'file',
    name: fileName,
    filePath: filePath,
    summary: summary || 'Python module in the paper trading engine.',
    tags: tags || ['python'],
    complexity: complexity
  };

  addNode(fileNode);

  // Create class nodes for significant classes
  if (result.classes && result.classes.length > 0) {
    result.classes.forEach(cls => {
      const classId = 'class:' + filePath + ':' + cls.name;
      const lineCount = cls.endLine - cls.startLine;

      // Significance filter: 2+ methods or 20+ lines
      if ((cls.methods && cls.methods.length >= 2) || lineCount >= 20) {
        let classSummary = cls.name + ' class with ' + (cls.methods ? cls.methods.length : 0) + ' methods.';
        let classComplexity = complexity;
        if (lineCount > 100) classComplexity = 'complex';
        else if (lineCount > 30) classComplexity = 'moderate';

        const classNode = {
          id: classId,
          type: 'class',
          name: cls.name,
          filePath: filePath,
          lineRange: [cls.startLine, cls.endLine],
          summary: classSummary,
          tags: ['class', 'stateful'],
          complexity: classComplexity
        };
        addNode(classNode);

        // Add contains edge
        addEdge({
          source: fileId,
          target: classId,
          type: 'contains',
          direction: 'forward',
          weight: 1.0
        });
      }
    });
  }

  // Create function nodes for significant functions
  if (result.functions && result.functions.length > 0) {
    result.functions.forEach(func => {
      const funcId = 'function:' + filePath + ':' + func.name;
      const lineCount = func.endLine - func.startLine;

      // Significance filter: 10+ lines
      if (lineCount >= 10) {
        const funcComplexity = lineCount > 50 ? 'complex' : (lineCount > 20 ? 'moderate' : 'simple');
        const funcNode = {
          id: funcId,
          type: 'function',
          name: func.name,
          filePath: filePath,
          lineRange: [func.startLine, func.endLine],
          summary: 'Function ' + func.name + ' in ' + fileName + '.',
          tags: ['function'],
          complexity: funcComplexity
        };
        addNode(funcNode);

        // Add contains edge
        addEdge({
          source: fileId,
          target: funcId,
          type: 'contains',
          direction: 'forward',
          weight: 1.0
        });
      }
    });
  }

  // Add export edges for exported items
  if (result.exports && result.exports.length > 0) {
    result.exports.forEach(exp => {
      // Find if this was added as a class node
      const classMatch = result.classes && result.classes.find(c => c.name === exp.name);
      if (classMatch) {
        const classId = 'class:' + filePath + ':' + exp.name;
        if (addedNodeIds.has(classId)) {
          addEdge({
            source: fileId,
            target: classId,
            type: 'exports',
            direction: 'forward',
            weight: 0.8
          });
        }
      }
    });
  }
});

// Add import edges
Object.entries(batchImportData).forEach(([sourceFile, importedFiles]) => {
  const sourceId = 'file:' + sourceFile;
  importedFiles.forEach(targetFile => {
    const targetId = 'file:' + targetFile;
    addEdge({
      source: sourceId,
      target: targetId,
      type: 'imports',
      direction: 'forward',
      weight: 0.7
    });
  });
});

// Output
const output = {
  nodes: nodes,
  edges: edges
};

const outputFile = path.join(UA_DIR, 'intermediate', `batch-${batchIndex}.json`);
fs.writeFileSync(outputFile, JSON.stringify(output, null, 2));
console.log('Output written to batch-' + batchIndex + '.json');
console.log('Nodes:', nodes.length);
console.log('Edges:', edges.length);
