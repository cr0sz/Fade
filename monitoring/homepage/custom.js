/* ── Market Intelligence — Dashboard shell (matches Stitch reference) ── */

(function () {
  'use strict';

  /* ── Sparkline generator (ported from Stitch reference) ── */
  function spark(vals, w, h, pad) {
    w = w || 120; h = h || 30; pad = pad || 3;
    var min = Math.min.apply(null, vals);
    var max = Math.max.apply(null, vals);
    var range = (max - min) || 1;
    var step = (w - pad * 2) / (vals.length - 1);
    return vals.map(function (v, i) {
      var x = pad + i * step;
      var y = pad + (h - pad * 2) * (1 - (v - min) / range);
      return (i === 0 ? 'M' : 'L') + x.toFixed(1) + ' ' + y.toFixed(1);
    }).join(' ');
  }

  var SPARK_A = spark([4,6,5,8,6,9,7,10,8,12,9,11,13,12,15]);
  var SPARK_B = spark([8,6,7,5,6,4,5,7,6,5,8,6,9,7,10]);
  var SPARK_C = spark([3,5,4,6,8,7,9,8,11,9,12,10,13,12,14]);

  /* ── SVG helpers ── */
  var NS = 'http://www.w3.org/2000/svg';

  function svgEl(tag, attrs) {
    var el = document.createElementNS(NS, tag);
    Object.keys(attrs).forEach(function (k) { el.setAttribute(k, attrs[k]); });
    return el;
  }

  function sparkSvg(d, h) {
    var svg = svgEl('svg', { viewBox: '0 0 120 30', preserveAspectRatio: 'none',
      class: 'mi-spark' + (h === 40 ? ' h40' : '') });
    var path = svgEl('path', { d: d, fill: 'none', stroke: '#52e08c', 'stroke-width': '1.6' });
    svg.appendChild(path);
    return svg;
  }

  /* ── Icon SVGs (inline, matching reference) ── */
  var ICONS = {
    dashboard:  '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/></svg>',
    trading:    '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 17 9 11 13 15 21 7"/><polyline points="15 7 21 7 21 13"/></svg>',
    research:   '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 3v5h5"/><path d="M19 8v11a1 1 0 0 1-1 1H6a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1h8z"/></svg>',
    infra:      '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="5" rx="1.5"/><rect x="3" y="15" width="18" height="5" rx="1.5"/><line x1="7" y1="6.5" x2="7" y2="6.5"/><line x1="7" y1="17.5" x2="7" y2="17.5"/></svg>',
    settings:   '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><line x1="4" y1="8" x2="20" y2="8"/><circle cx="9" cy="8" r="2.4" fill="#070a08"/><line x1="4" y1="16" x2="20" y2="16"/><circle cx="15" cy="16" r="2.4" fill="#070a08"/></svg>',
    account:    '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="8" r="3.6"/><path d="M5 20c0-3.6 3.1-6 7-6s7 2.4 7 6"/></svg>',
    search:     '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#778a7f" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><line x1="21" y1="21" x2="16.5" y2="16.5"/></svg>',
    calendar:   '<svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="#778a7f" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4.5" width="18" height="16" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="8" y1="2.5" x2="8" y2="6"/><line x1="16" y1="2.5" x2="16" y2="6"/></svg>',
    graph:      '<svg width="62" height="62" viewBox="0 0 24 24" fill="none" stroke="#5fb98a" stroke-width="1.3" style="position:absolute;right:20px;top:50%;transform:translateY(-50%);opacity:.7;"><circle cx="12" cy="12" r="3"/><circle cx="5" cy="6" r="2"/><circle cx="19" cy="6" r="2"/><circle cx="5" cy="18" r="2"/><circle cx="19" cy="18" r="2"/><line x1="6.5" y1="7" x2="10" y2="10.5"/><line x1="17.5" y1="7" x2="14" y2="10.5"/><line x1="6.5" y1="17" x2="10" y2="13.5"/><line x1="17.5" y1="17" x2="14" y2="13.5"/></svg>',
    monitor:    '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="14" rx="2"/><line x1="3" y1="20" x2="21" y2="20"/></svg>',
    refresh:    '<svg width="19" height="19" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-3-6.7"/><polyline points="21 3 21 8 16 8"/></svg>',
  };

  /* ── Build element helpers ── */
  function el(tag, props, children) {
    var e = document.createElement(tag);
    if (props) {
      if (props.className) e.className = props.className;
      if (props.id) e.id = props.id;
      if (props.style) e.style.cssText = props.style;
      if (props.href) { e.href = props.href; }
      if (props.innerHTML) e.innerHTML = props.innerHTML;
      if (props.placeholder) e.placeholder = props.placeholder;
    }
    if (children) {
      children.forEach(function (c) {
        if (c) e.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
      });
    }
    return e;
  }

  function div(cls, children, style) {
    return el('div', { className: cls, style: style }, children);
  }

  function html(tag, markup, cls) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    e.innerHTML = markup;
    return e;
  }

  /* ── Sidebar ── */
  function buildSidebar() {
    var nav = el('nav', {}, [
      navLink('Dashboard', ICONS.dashboard, true),
      navLink('Trading',   ICONS.trading),
      navLink('Research',  ICONS.research),
      navLink('Infrastructure', ICONS.infra),
      navLink('Settings',  ICONS.settings),
      navLink('Account',   ICONS.account),
    ]);

    var status = div('mi-status', [
      div('mi-status-dot'),
      el('span', { className: 'mi-status-label' }, ['SYSTEM ONLINE']),
    ]);

    var sidebar = el('aside', { id: 'mi-sidebar' }, [
      div('mi-brand', [
        div('mi-logo', ['Ai']),
        div('', [
          div('mi-brand-name', ['Enhanced Stealth']),
          div('mi-brand-sub', ['Trading Dashboard v3']),
        ]),
      ]),
      nav,
      status,
    ]);

    return sidebar;
  }

  var NAV_URLS = {
    'Dashboard':      'http://localhost:8000/dashboard',
    'Trading':        'http://localhost:8000/dashboard',
    'Research':       'http://localhost:8000/graphify',
    'Infrastructure': 'http://localhost:3002',
    'Settings':       '#',
    'Account':        'https://www.binance.com/en/futures',
  };

  function navLink(label, iconHtml, active) {
    var href = NAV_URLS[label] || '#';
    var a = el('a', { href: href, className: active ? 'active' : '' });
    var ico = document.createElement('span');
    ico.innerHTML = iconHtml;
    ico.style.cssText = 'display:flex;align-items:center;flex:none;';
    a.appendChild(ico);
    a.appendChild(document.createTextNode(label));
    return a;
  }

  /* ── Top bar ── */
  function buildTopBar() {
    var metrics = div('', [
      metricCard('CPU',  '34%',         'r1'),
      metricCard('RAM',  '3.4 GiB Free','r2'),
      metricCard('DISK', '88.8 GB Free','r3'),
    ], 'display:flex;gap:10px;flex:none;');

    var search = div('', [
      html('span', ICONS.search),
      el('input', { placeholder: 'Search modules, trades, signals…',
        style: 'flex:1;background:none;border:none;outline:none;color:#e6ede8;font-family:Manrope,sans-serif;font-size:14.5px;' }),
    ], 'flex:1;display:flex;align-items:center;gap:11px;padding:0 18px;height:56px;border-radius:14px;background:rgba(255,255,255,.025);border:1px solid rgba(120,200,160,.09);');

    var now = new Date();
    var dateStr = (now.getMonth()+1) + '/' + now.getDate() + '/' + String(now.getFullYear()).slice(2) +
      ', ' + now.getHours().toString().padStart(2,'0') + ':' + now.getMinutes().toString().padStart(2,'0');

    var datePill = div('', [
      html('span', ICONS.calendar),
      el('span', { style: 'font-family:"IBM Plex Mono",monospace;font-size:13.5px;color:#cfe0d6;' }, [dateStr]),
    ], 'flex:none;display:flex;align-items:center;gap:10px;padding:0 18px;height:56px;border-radius:14px;background:rgba(255,255,255,.025);border:1px solid rgba(120,200,160,.09);');

    return el('div', { id: 'mi-topbar' }, [metrics, search, datePill]);
  }

  function metricCard(label, val, ringClass) {
    return div('mi-metric', [
      div('mi-metric-ring ' + ringClass),
      div('', [
        div('mi-metric-lbl', [label]),
        div('mi-metric-val', [val]),
      ]),
    ]);
  }

  /* ── Section label ── */
  function sectionLabel(text) {
    return div('mi-section-label', [text]);
  }

  /* ── Cards ── */
  var CARD_URLS = {
    'Dashboard':       'http://localhost:8000/dashboard',
    'Engine Intel':    'http://localhost:8000/dashboard#engine-intel',
    'Signal Flow':     'http://localhost:8000/dashboard#signals',
    'Knowledge Graph': 'http://localhost:8000/graphify',
    'Open WebUI':      'http://localhost:3003',
    'Uptime Kuma':     'http://localhost:3002',
    'Qdrant':          'http://localhost:6333/dashboard',
    'Binance':         'https://www.binance.com/en/my/orders/futures/closedOrders',
    'Miniflux':        'http://localhost:4000',
    'Job Scraper':     'http://localhost:8000/dashboard',
  };

  function cardLink(title, inner) {
    var href = CARD_URLS[title] || '#';
    var a = document.createElement('a');
    a.href = href;
    a.style.cssText = 'display:contents;text-decoration:none;color:inherit;';
    a.appendChild(inner);
    return a;
  }

  function tradingCard(title, sub, badgeText, sparkData, cardClass) {
    var badge = el('span', { className: 'mi-badge' }, [badgeText]);
    var header = div('mi-card-header', [
      div('mi-card-title', [title]),
      badge,
    ]);
    var c = div('mi-card ' + (cardClass || 'trading-2'), [
      header,
      div('mi-card-sub', [sub]),
    ]);
    c.appendChild(sparkSvg(sparkData));
    c.style.cursor = 'pointer';
    c.addEventListener('click', function() { window.open(CARD_URLS[title] || '#', '_blank'); });
    return c;
  }

  function researchCard(title, sub, badgeText, iconHtml) {
    var header = div('mi-card-header', [
      div('mi-card-title sm', [title]),
      badgeText ? el('span', { className: 'mi-badge' }, [badgeText]) : null,
    ]);
    var c = div('mi-card research', [
      div('', [header, div('mi-card-sub', [sub])]),
    ]);
    if (iconHtml) {
      var ico = document.createElement('span');
      ico.innerHTML = iconHtml;
      c.appendChild(ico);
    }
    if (CARD_URLS[title]) {
      c.style.cursor = 'pointer';
      c.addEventListener('click', function() { window.open(CARD_URLS[title], '_blank'); });
    }
    return c;
  }

  function infraCard(title, sub, badgeText, sparkData, badgeClass) {
    var badge = el('span', { className: 'mi-badge ' + (badgeClass || '') }, [badgeText]);
    var header = div('mi-card-header', [
      div('mi-card-title sm', [title]),
      badge,
    ]);
    var c = div('mi-card infra', [header, div('mi-card-sub', [sub])]);
    if (sparkData) c.appendChild(sparkSvg(sparkData, 40));
    if (CARD_URLS[title]) {
      c.style.cursor = 'pointer';
      c.addEventListener('click', function() { window.open(CARD_URLS[title], '_blank'); });
    }
    return c;
  }

  /* ── Quick links ── */
  function quickLink(label, urlText, iconBg, iconContent, href) {
    var ico = div('mi-ql-icon', [], 'background:' + iconBg + ';');
    ico.innerHTML = iconContent;
    var info = div('', [
      div('mi-ql-title', [label]),
      div('mi-ql-url', [urlText]),
    ]);
    var a = el('a', { className: 'mi-ql-link', href: href || '#' }, [ico, info]);
    if (!href || href === '#') a.addEventListener('click', function(e){ e.preventDefault(); });
    return a;
  }

  /* ── Main content ── */
  function buildMain() {
    var glowA = div('mi-glow-a');
    var glowB = div('mi-glow-b');

    /* TRADING */
    var tradingGrid = div('mi-grid', [
      tradingCard('Dashboard',   'Positions · signals · news · autopsy', '4 MS', SPARK_A, 'trading-1'),
      tradingCard('Engine Intel','Trust scores · accuracy · win rates',  '2 MS', SPARK_B, 'trading-2'),
      tradingCard('Signal Flow', 'Live signal trace · dedup · scoring',  '3 MS', SPARK_C, 'trading-2'),
    ]);

    /* RESEARCH */
    var researchGrid = div('mi-grid', [
      researchCard('Knowledge Graph',  'Codebase brain · 3223 nodes · 211 communities', null, ICONS.graph),
      researchCard('Open WebUI',       'Local LLM chat · qwen2.5:7b',                   '10 MS', null),
      div('mi-card research'),  /* empty slot */
    ]);

    /* INFRASTRUCTURE */
    var infraGrid = div('mi-grid', [
      infraCard('Uptime Kuma', 'Service uptime · alerting · CPU load 32%', '6 MS',  SPARK_A),
      infraCard('Qdrant',      'Vector store · 21k news articles · RAM 25%','4 MS',  SPARK_C),
      infraCard('Binance',     'Futures closed orders',                      '305 MS', null, 'yellow'),
    ]);

    /* QUICK LINKS */
    var ql = div('', [
      quickLink('Binance',   'www.binance.com',       'rgba(220,180,90,.12)',
        '<span style="font-family:Space Grotesk,sans-serif;font-weight:700;font-size:16px;color:#e8c06a;">B</span>',
        'https://www.binance.com'),
      quickLink('Dashboard', 'localhost',        'rgba(80,210,140,.12)',
        ICONS.monitor.replace('stroke="currentColor"','stroke="#7ff0ad"'),
        'http://localhost:8000/dashboard'),
      div('', [
        el('button', {
          style: 'width:46px;height:46px;border-radius:50%;background:rgba(255,255,255,.022);border:1px solid rgba(120,200,160,.12);color:#84c9a3;cursor:pointer;display:flex;align-items:center;justify-content:center;margin-left:auto;',
          innerHTML: ICONS.refresh,
        }),
      ], 'margin-left:auto;display:flex;align-items:center;'),
    ], 'display:flex;align-items:center;gap:18px;');

    var footer = div('', ['v3.0 · Enhanced Stealth · Market Intelligence'], 'font-family:"IBM Plex Mono",monospace;font-size:11.5px;color:#566459;text-align:right;margin-top:26px;');

    var content = el('div', { id: 'mi-content' }, [
      buildTopBar(),
      sectionLabel('TRADING'),
      tradingGrid,
      sectionLabel('RESEARCH'),
      researchGrid,
      sectionLabel('INFRASTRUCTURE'),
      infraGrid,
      sectionLabel('QUICK LINKS'),
      ql,
      footer,
    ]);

    var main = el('main', { id: 'mi-main' }, [glowA, glowB, content]);
    return main;
  }

  /* ── Bootstrap ── */
  function init() {
    /* Hide everything Homepage rendered */
    var body = document.body;

    /* Wrap in our app shell */
    var app = el('div', { id: 'mi-app' }, [
      buildSidebar(),
      buildMain(),
    ]);

    /* Inject at top of body; CSS hides the rest */
    body.insertBefore(app, body.firstChild);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
