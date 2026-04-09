/**
 * Colab91 Supplier Universe Cloud Sync — Cloudflare Worker
 *
 * Standalone web dashboard for uploading, versioning, and browsing
 * supplier universe DuckDB files synced to D1 (SQLite).
 *
 * Endpoints:
 *   GET  /                              — dashboard webpage
 *   POST /api/sync                      — upload suppliers JSON, create new version
 *   GET  /api/versions                  — list all versions
 *   PUT  /api/versions/:id/notes        — update version notes
 *   GET  /api/versions/:id/suppliers    — get suppliers for a version
 *   DELETE /api/versions/:id            — delete a version
 *
 * D1 Database: supplier-universe
 */

interface Env {
  DB: D1Database;
}

interface SupplierRow {
  id: string;
  supplier_name: string;
  aliases: string | null;
  parent_supplier_id: string | null;
  parent_name: string | null;
  supplier_info: string | null;
  mapped_taxonomy_paths: string | null;
  category_a_supplier: number;
  created_at: string | null;
  updated_at: string | null;
}

interface SyncRequest {
  suppliers: SupplierRow[];
  notes?: string;
}

interface VersionRow {
  id: number;
  version_number: number;
  created_at: string;
  notes: string | null;
  supplier_count: number;
  status: string;
}

interface DiffResult {
  added: { supplier_name: string; id: string }[];
  removed: { supplier_name: string; id: string }[];
  updated: { supplier_name: string; id: string; changes: string[] }[];
  summary: { added: number; removed: number; updated: number; unchanged: number };
}

// ─── D1 Schema Initialization ──────────────────────────────────────────────

async function initializeSchema(db: D1Database): Promise<void> {
  await db.batch([
    db.prepare(`
      CREATE TABLE IF NOT EXISTS versions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        version_number INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        notes TEXT,
        supplier_count INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'active'
      )
    `),
    db.prepare(`
      CREATE TABLE IF NOT EXISTS suppliers (
        id TEXT NOT NULL,
        version_id INTEGER NOT NULL,
        supplier_name TEXT NOT NULL,
        aliases TEXT,
        parent_supplier_id TEXT,
        parent_name TEXT,
        supplier_info TEXT,
        mapped_taxonomy_paths TEXT,
        category_a_supplier INTEGER DEFAULT 0,
        created_at TEXT,
        updated_at TEXT,
        FOREIGN KEY (version_id) REFERENCES versions(id),
        PRIMARY KEY (id, version_id)
      )
    `),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_suppliers_version ON suppliers(version_id)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_suppliers_name_version ON suppliers(supplier_name, version_id)`),
  ]);
}

// ─── Route Handlers ─────────────────────────────────────────────────────────

async function handleSync(request: Request, db: D1Database): Promise<Response> {
  let body: SyncRequest;
  try {
    body = await request.json();
  } catch {
    return jsonResponse({ error: 'Invalid JSON body' }, 400);
  }

  if (!body.suppliers || !Array.isArray(body.suppliers) || body.suppliers.length === 0) {
    return jsonResponse({ error: 'suppliers array is required and must not be empty' }, 400);
  }

  // Get previous active version for diff
  const prevVersion = await db.prepare(
    "SELECT id, version_number FROM versions WHERE status = 'active' ORDER BY version_number DESC LIMIT 1"
  ).first<{ id: number; version_number: number }>();

  // Get next version number
  const latest = await db.prepare(
    'SELECT MAX(version_number) as max_v FROM versions'
  ).first<{ max_v: number | null }>();
  const nextVersion = (latest?.max_v ?? 0) + 1;

  // Mark previous active versions as superseded
  await db.prepare(
    "UPDATE versions SET status = 'superseded' WHERE status = 'active'"
  ).run();

  // Create new version
  const versionResult = await db.prepare(
    'INSERT INTO versions (version_number, notes, supplier_count) VALUES (?, ?, ?)'
  ).bind(nextVersion, body.notes || null, body.suppliers.length).run();

  const versionId = versionResult.meta.last_row_id;

  // Batch-insert suppliers (D1 allows up to 100 statements per batch)
  const BATCH_SIZE = 100;
  for (let i = 0; i < body.suppliers.length; i += BATCH_SIZE) {
    const chunk = body.suppliers.slice(i, i + BATCH_SIZE);
    const stmts = chunk.map((s) =>
      db.prepare(
        `INSERT INTO suppliers (id, version_id, supplier_name, aliases, parent_supplier_id, parent_name, supplier_info, mapped_taxonomy_paths, category_a_supplier, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      ).bind(
        s.id,
        versionId,
        s.supplier_name,
        s.aliases || null,
        s.parent_supplier_id || null,
        s.parent_name || null,
        s.supplier_info || null,
        s.mapped_taxonomy_paths || null,
        s.category_a_supplier ? 1 : 0,
        s.created_at || null,
        s.updated_at || null,
      )
    );
    await db.batch(stmts);
  }

  // Compute diff against previous version if one exists
  let diff: DiffResult | null = null;
  if (prevVersion) {
    diff = await computeDiff(db, prevVersion.id, Number(versionId));
  }

  return jsonResponse({
    success: true,
    versionNumber: nextVersion,
    versionId,
    supplierCount: body.suppliers.length,
    diff,
  });
}

async function computeDiff(db: D1Database, fromVersionId: number, toVersionId: number): Promise<DiffResult> {
  // Get suppliers from both versions keyed by supplier_name
  const { results: fromSuppliers } = await db.prepare(
    'SELECT id, supplier_name, aliases, parent_name, supplier_info, mapped_taxonomy_paths, category_a_supplier FROM suppliers WHERE version_id = ?'
  ).bind(fromVersionId).all<SupplierRow>();

  const { results: toSuppliers } = await db.prepare(
    'SELECT id, supplier_name, aliases, parent_name, supplier_info, mapped_taxonomy_paths, category_a_supplier FROM suppliers WHERE version_id = ?'
  ).bind(toVersionId).all<SupplierRow>();

  const fromMap = new Map((fromSuppliers || []).map(s => [s.supplier_name.toLowerCase(), s]));
  const toMap = new Map((toSuppliers || []).map(s => [s.supplier_name.toLowerCase(), s]));

  const added: DiffResult['added'] = [];
  const removed: DiffResult['removed'] = [];
  const updated: DiffResult['updated'] = [];
  let unchanged = 0;

  // Find added and updated
  for (const [key, toS] of toMap) {
    const fromS = fromMap.get(key);
    if (!fromS) {
      added.push({ supplier_name: toS.supplier_name, id: toS.id });
    } else {
      // Compare fields
      const changes: string[] = [];
      if ((fromS.aliases || '') !== (toS.aliases || '')) changes.push('aliases');
      if ((fromS.parent_name || '') !== (toS.parent_name || '')) changes.push('parent_name');
      if ((fromS.supplier_info || '') !== (toS.supplier_info || '')) changes.push('supplier_info');
      if ((fromS.mapped_taxonomy_paths || '') !== (toS.mapped_taxonomy_paths || '')) changes.push('taxonomy');
      if ((fromS.category_a_supplier || 0) !== (toS.category_a_supplier || 0)) changes.push('category_a');

      if (changes.length > 0) {
        updated.push({ supplier_name: toS.supplier_name, id: toS.id, changes });
      } else {
        unchanged++;
      }
    }
  }

  // Find removed
  for (const [key, fromS] of fromMap) {
    if (!toMap.has(key)) {
      removed.push({ supplier_name: fromS.supplier_name, id: fromS.id });
    }
  }

  return {
    added,
    removed,
    updated,
    summary: { added: added.length, removed: removed.length, updated: updated.length, unchanged },
  };
}

async function handleDiff(url: URL, db: D1Database): Promise<Response> {
  const fromId = url.searchParams.get('from');
  const toId = url.searchParams.get('to');

  if (!fromId || !toId) {
    return jsonResponse({ error: 'from and to version IDs are required' }, 400);
  }

  // Validate both versions exist
  const fromVersion = await db.prepare('SELECT id, version_number FROM versions WHERE id = ?').bind(fromId).first<VersionRow>();
  const toVersion = await db.prepare('SELECT id, version_number FROM versions WHERE id = ?').bind(toId).first<VersionRow>();

  if (!fromVersion || !toVersion) {
    return jsonResponse({ error: 'One or both versions not found' }, 404);
  }

  const diff = await computeDiff(db, Number(fromId), Number(toId));

  return jsonResponse({
    from: { id: fromVersion.id, version_number: fromVersion.version_number },
    to: { id: toVersion.id, version_number: toVersion.version_number },
    diff,
  });
}

async function handleGetVersions(db: D1Database): Promise<Response> {
  const { results } = await db.prepare(
    'SELECT id, version_number, created_at, notes, supplier_count, status FROM versions ORDER BY version_number DESC'
  ).all<VersionRow>();

  return jsonResponse({ versions: results || [] });
}

async function handleUpdateNotes(versionId: string, request: Request, db: D1Database): Promise<Response> {
  let body: { notes: string };
  try {
    body = await request.json();
  } catch {
    return jsonResponse({ error: 'Invalid JSON body' }, 400);
  }

  const existing = await db.prepare('SELECT id FROM versions WHERE id = ?').bind(versionId).first();
  if (!existing) {
    return jsonResponse({ error: 'Version not found' }, 404);
  }

  await db.prepare('UPDATE versions SET notes = ? WHERE id = ?').bind(body.notes, versionId).run();

  return jsonResponse({ success: true });
}

async function handleGetSuppliers(versionId: string, db: D1Database): Promise<Response> {
  const existing = await db.prepare('SELECT id FROM versions WHERE id = ?').bind(versionId).first();
  if (!existing) {
    return jsonResponse({ error: 'Version not found' }, 404);
  }

  const { results } = await db.prepare(
    'SELECT id, supplier_name, aliases, parent_name, supplier_info, mapped_taxonomy_paths, category_a_supplier, created_at, updated_at FROM suppliers WHERE version_id = ? ORDER BY supplier_name'
  ).bind(versionId).all<SupplierRow>();

  return jsonResponse({ suppliers: results || [] });
}

async function handleDeleteVersion(versionId: string, db: D1Database): Promise<Response> {
  const existing = await db.prepare('SELECT id FROM versions WHERE id = ?').bind(versionId).first();
  if (!existing) {
    return jsonResponse({ error: 'Version not found' }, 404);
  }

  await db.batch([
    db.prepare('DELETE FROM suppliers WHERE version_id = ?').bind(versionId),
    db.prepare('DELETE FROM versions WHERE id = ?').bind(versionId),
  ]);

  return jsonResponse({ success: true });
}

// ─── Helpers ────────────────────────────────────────────────────────────────

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    },
  });
}

// ─── Dashboard HTML ─────────────────────────────────────────────────────────

const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Supplier Universe — Cloud Sync</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f3f4f6; color: #1f2937; }
  .container { max-width: 1100px; margin: 0 auto; padding: 24px; }
  h1 { font-size: 26px; font-weight: 700; margin-bottom: 4px; }
  h2 { font-size: 20px; font-weight: 600; margin-bottom: 12px; }
  .subtitle { color: #6b7280; font-size: 14px; margin-bottom: 24px; }

  /* Cards */
  .card { background: #fff; border-radius: 12px; box-shadow: 0 1px 3px rgba(0,0,0,.1); padding: 24px; margin-bottom: 20px; }

  /* Upload Zone */
  .upload-zone { border: 2px dashed #d1d5db; border-radius: 12px; padding: 48px 24px; text-align: center; cursor: pointer; transition: all .2s; background: #fafafa; }
  .upload-zone:hover, .upload-zone.dragover { border-color: #2563eb; background: #eff6ff; }
  .upload-zone p { color: #6b7280; font-size: 15px; margin-top: 8px; }
  .upload-zone .icon { font-size: 40px; color: #9ca3af; }
  .upload-zone .file-name { color: #2563eb; font-weight: 600; font-size: 16px; margin-top: 8px; }

  /* Preview */
  #preview { display: none; margin-top: 16px; padding: 16px; background: #f0fdf4; border-radius: 8px; border: 1px solid #bbf7d0; }
  #preview .count { font-size: 28px; font-weight: 700; color: #166534; }
  #preview .label { color: #15803d; font-size: 14px; }

  /* Notes input */
  .notes-input { width: 100%; padding: 10px 12px; border: 1px solid #d1d5db; border-radius: 8px; font-size: 14px; margin-top: 12px; resize: vertical; min-height: 60px; font-family: inherit; }

  /* Sync button */
  .sync-btn { display: inline-flex; align-items: center; gap: 8px; padding: 12px 24px; border: none; border-radius: 8px; background: #2563eb; color: #fff; font-weight: 600; cursor: pointer; font-size: 15px; margin-top: 12px; transition: background .15s; }
  .sync-btn:hover { background: #1d4ed8; }
  .sync-btn:disabled { background: #93c5fd; cursor: not-allowed; }

  /* Progress */
  .progress-wrap { display: none; margin-top: 12px; }
  .progress-bar { height: 6px; background: #e5e7eb; border-radius: 3px; overflow: hidden; }
  .progress-fill { height: 100%; background: #2563eb; border-radius: 3px; transition: width .3s; }
  .progress-text { font-size: 13px; color: #6b7280; margin-top: 4px; }

  /* Table */
  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; padding: 10px 14px; background: #f9fafb; font-size: 12px; text-transform: uppercase; letter-spacing: .05em; color: #6b7280; border-bottom: 1px solid #e5e7eb; }
  td { padding: 10px 14px; border-bottom: 1px solid #f3f4f6; font-size: 14px; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: #f9fafb; }
  .mono { font-family: 'SF Mono', Monaco, Consolas, monospace; font-size: 13px; }

  /* Badges */
  .badge { display: inline-block; padding: 2px 8px; border-radius: 9999px; font-size: 12px; font-weight: 600; }
  .badge-green { background: #dcfce7; color: #166534; }
  .badge-gray { background: #f3f4f6; color: #6b7280; }

  /* Buttons */
  .btn { padding: 6px 12px; border: 1px solid #d1d5db; border-radius: 6px; font-size: 13px; cursor: pointer; background: #fff; transition: all .15s; }
  .btn:hover { background: #f9fafb; }
  .btn-primary { background: #2563eb; color: #fff; border-color: #2563eb; }
  .btn-primary:hover { background: #1d4ed8; }
  .btn-danger { color: #dc2626; border-color: #fca5a5; }
  .btn-danger:hover { background: #fef2f2; }
  .btn-sm { padding: 4px 8px; font-size: 12px; }
  .actions { display: flex; gap: 6px; }

  /* Modal */
  .modal-bg { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.4); z-index: 50; justify-content: center; align-items: center; }
  .modal-bg.active { display: flex; }
  .modal { background: #fff; padding: 24px; border-radius: 12px; width: 100%; max-width: 1100px; max-height: 85vh; box-shadow: 0 20px 60px rgba(0,0,0,.2); display: flex; flex-direction: column; }
  .modal h2 { font-size: 18px; font-weight: 600; margin-bottom: 12px; }
  .modal-body { overflow-y: auto; flex: 1; }
  .modal-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 12px; padding-top: 12px; border-top: 1px solid #e5e7eb; }
  .modal input[type="text"], .modal textarea { width: 100%; padding: 8px 10px; border: 1px solid #d1d5db; border-radius: 6px; font-size: 14px; font-family: inherit; }

  /* Search */
  .search-bar { display: flex; gap: 8px; margin-bottom: 12px; }
  .search-bar input { flex: 1; padding: 8px 12px; border: 1px solid #d1d5db; border-radius: 8px; font-size: 14px; }

  .empty { text-align: center; padding: 48px; color: #9ca3af; }

  /* Toast */
  .toast { position: fixed; bottom: 24px; right: 24px; background: #1f2937; color: #fff; padding: 12px 20px; border-radius: 8px; font-size: 14px; z-index: 100; display: none; }

  /* Supplier info expandable */
  .info-toggle { color: #2563eb; cursor: pointer; font-size: 12px; }
  .info-content { display: none; margin-top: 6px; padding: 8px; background: #f9fafb; border-radius: 6px; font-size: 12px; white-space: pre-wrap; word-break: break-all; max-height: 200px; overflow-y: auto; }
  .info-content.open { display: block; }

  /* Loading spinner */
  .spinner { display: inline-block; width: 16px; height: 16px; border: 2px solid #fff; border-top-color: transparent; border-radius: 50%; animation: spin .6s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }

  .topbar { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 24px; }

  /* Diff styles */
  .diff-summary { display: flex; gap: 16px; margin-bottom: 16px; }
  .diff-stat { padding: 12px 20px; border-radius: 8px; text-align: center; flex: 1; }
  .diff-stat .num { font-size: 28px; font-weight: 700; }
  .diff-stat .lbl { font-size: 12px; text-transform: uppercase; letter-spacing: .05em; margin-top: 2px; }
  .diff-added { background: #f0fdf4; border: 1px solid #bbf7d0; }
  .diff-added .num { color: #166534; }
  .diff-added .lbl { color: #15803d; }
  .diff-removed { background: #fef2f2; border: 1px solid #fecaca; }
  .diff-removed .num { color: #991b1b; }
  .diff-removed .lbl { color: #dc2626; }
  .diff-updated { background: #fffbeb; border: 1px solid #fde68a; }
  .diff-updated .num { color: #92400e; }
  .diff-updated .lbl { color: #b45309; }
  .diff-unchanged { background: #f9fafb; border: 1px solid #e5e7eb; }
  .diff-unchanged .num { color: #6b7280; }
  .diff-unchanged .lbl { color: #9ca3af; }
  .diff-section { margin-bottom: 16px; }
  .diff-section h3 { font-size: 14px; font-weight: 600; margin-bottom: 8px; padding: 6px 0; border-bottom: 1px solid #e5e7eb; }
  .diff-list { list-style: none; padding: 0; max-height: 200px; overflow-y: auto; }
  .diff-list li { padding: 6px 10px; font-size: 13px; border-radius: 4px; margin-bottom: 2px; }
  .diff-list li.add { background: #f0fdf4; color: #166534; }
  .diff-list li.rem { background: #fef2f2; color: #991b1b; }
  .diff-list li.upd { background: #fffbeb; color: #92400e; }
  .diff-list .changes { font-size: 11px; color: #6b7280; margin-left: 8px; }
  .compare-select { padding: 6px 10px; border: 1px solid #d1d5db; border-radius: 6px; font-size: 13px; background: #fff; }
</style>
</head>
<body>
<div class="container">
  <div class="topbar">
    <div>
      <h1>Supplier Universe</h1>
      <p class="subtitle">Cloud Sync Dashboard</p>
    </div>
  </div>

  <!-- Upload Section -->
  <div class="card">
    <h2>Upload Universe DB</h2>
    <div class="upload-zone" id="dropZone" onclick="document.getElementById('fileInput').click()">
      <div class="icon">&#128451;</div>
      <p>Drag &amp; drop your <strong>supplier_universe.db</strong> file here, or click to browse</p>
      <div class="file-name" id="fileName"></div>
    </div>
    <input type="file" id="fileInput" accept=".db,.duckdb" style="display:none" />

    <div id="preview">
      <div class="count" id="supplierCount">0</div>
      <div class="label">suppliers found in database</div>
    </div>

    <textarea class="notes-input" id="syncNotes" placeholder="Add notes for this sync (e.g., &quot;Q1 2026 supplier updates&quot;)"></textarea>

    <button class="sync-btn" id="syncBtn" disabled onclick="doSync()">
      Sync to Cloud
    </button>

    <div class="progress-wrap" id="progressWrap">
      <div class="progress-bar"><div class="progress-fill" id="progressFill" style="width:0%"></div></div>
      <div class="progress-text" id="progressText">Preparing...</div>
    </div>
  </div>

  <!-- Version History -->
  <div class="card">
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
      <h2 style="margin-bottom:0">Version History</h2>
      <button class="btn" onclick="loadVersions()">Refresh</button>
    </div>
    <div id="versionsTable"></div>
  </div>

  <!-- Compare Versions -->
  <div class="card" id="compareCard" style="display:none">
    <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:12px;">
      <h2 style="margin-bottom:0">Compare Versions</h2>
      <div style="display:flex; gap:8px; align-items:center;">
        <select class="compare-select" id="compareFrom"></select>
        <span style="color:#6b7280">vs</span>
        <select class="compare-select" id="compareTo"></select>
        <button class="btn btn-primary" onclick="compareVersions()">Compare</button>
      </div>
    </div>
  </div>
</div>

<!-- Suppliers Modal -->
<div class="modal-bg" id="suppliersModal">
  <div class="modal">
    <h2 id="suppliersModalTitle">Suppliers — Version</h2>
    <div class="search-bar">
      <input type="text" id="supplierSearch" placeholder="Search suppliers..." oninput="filterSuppliers()" />
    </div>
    <div class="modal-body" id="suppliersTableWrap"></div>
    <div class="modal-actions">
      <button class="btn" onclick="closeSuppliersModal()">Close</button>
    </div>
  </div>
</div>

<!-- Notes Modal -->
<div class="modal-bg" id="notesModal">
  <div class="modal" style="max-width:500px">
    <h2>Edit Notes</h2>
    <textarea id="notesInput" style="width:100%;min-height:100px;padding:10px;border:1px solid #d1d5db;border-radius:8px;font-size:14px;font-family:inherit;resize:vertical"></textarea>
    <input type="hidden" id="notesVersionId" />
    <div class="modal-actions">
      <button class="btn" onclick="closeNotesModal()">Cancel</button>
      <button class="btn btn-primary" onclick="saveNotes()">Save</button>
    </div>
  </div>
</div>

<!-- Diff Modal -->
<div class="modal-bg" id="diffModal">
  <div class="modal" style="max-width:800px">
    <h2 id="diffModalTitle">Version Diff</h2>
    <div class="modal-body" id="diffContent"></div>
    <div class="modal-actions">
      <button class="btn" onclick="closeDiffModal()">Close</button>
    </div>
  </div>
</div>

<!-- Toast -->
<div class="toast" id="toast"></div>

<script>
// ─── Global state ───────────────────────────────────────────────────────────

var parsedSuppliers = [];
var versionNotesMap = {};
var allModalSuppliers = [];
var BASE = '';

// ─── Sync to Cloud ──────────────────────────────────────────────────────────

async function doSync() {
  var suppliers = window.parsedSuppliers || parsedSuppliers;
  if (!suppliers || suppliers.length === 0) return;

  var btn = document.getElementById('syncBtn');
  var progressWrap = document.getElementById('progressWrap');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> Syncing...';
  progressWrap.style.display = 'block';
  setProgress(20, 'Uploading ' + suppliers.length + ' suppliers...');

  try {
    var notes = document.getElementById('syncNotes').value.trim();
    var resp = await fetch(BASE + '/api/sync', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ suppliers: suppliers, notes: notes || null }),
    });

    if (!resp.ok) {
      const err = await resp.json();
      throw new Error(err.error || 'Sync failed');
    }

    const result = await resp.json();
    setProgress(100, 'Synced! Version ' + result.versionNumber + ' created with ' + result.supplierCount + ' suppliers');
    toast('Version ' + result.versionNumber + ' created successfully!');
    document.getElementById('syncNotes').value = '';
    loadVersions();

    // Show diff if available (not first sync)
    if (result.diff) {
      setTimeout(() => showSyncDiff(result), 500);
    }

    setTimeout(() => { progressWrap.style.display = 'none'; }, 2000);
  } catch (err) {
    setProgress(0, 'Error: ' + err.message);
    toast('Sync failed: ' + err.message);
  } finally {
    btn.disabled = false;
    btn.innerHTML = 'Sync to Cloud';
  }
};

// ─── Version History ─────────────────────────────────────────────────────────

async function loadVersions() {
  const resp = await fetch(BASE + '/api/versions');
  const data = await resp.json();
  renderVersions(data.versions || []);
}

function renderVersions(versions) {
  updateCompareDropdowns(versions);
  const wrap = document.getElementById('versionsTable');
  if (!versions.length) {
    document.getElementById('compareCard').style.display = 'none';
    wrap.innerHTML = '<div class="empty">No versions yet. Upload a database file to create the first version.</div>';
    return;
  }

  // Store notes in a JS map so we don't need to pass strings through HTML attributes
  versionNotesMap = {};
  for (const v of versions) {
    versionNotesMap[v.id] = v.notes || '';
  }

  let html = '<table><thead><tr><th>Version</th><th>Date & Time</th><th>Suppliers</th><th>Status</th><th>Actions</th></tr></thead><tbody>';
  for (const v of versions) {
    const date = new Date(v.created_at + 'Z').toLocaleString();
    const statusBadge = v.status === 'active'
      ? '<span class="badge badge-green">Active</span>'
      : '<span class="badge badge-gray">Superseded</span>';
    const hasNotes = v.notes && v.notes.trim().length > 0;

    html += '<tr>'
      + '<td class="mono">v' + v.version_number + '</td>'
      + '<td>' + date + '</td>'
      + '<td>' + v.supplier_count.toLocaleString() + '</td>'
      + '<td>' + statusBadge + '</td>'
      + '<td class="actions">'
      +   '<button class="btn btn-sm" onclick="viewSuppliers(' + v.id + ', ' + v.version_number + ')">View</button>'
      +   '<button class="btn btn-sm' + (hasNotes ? ' btn-primary' : '') + '" onclick="editNotes(' + v.id + ')">Notes' + (hasNotes ? ' *' : '') + '</button>'
      +   '<button class="btn btn-sm btn-danger" onclick="deleteVersion(' + v.id + ', ' + v.version_number + ')">Delete</button>'
      + '</td></tr>';
  }
  html += '</tbody></table>';
  wrap.innerHTML = html;
}

// ─── Supplier Browser ────────────────────────────────────────────────────────

async function viewSuppliers(versionId, versionNumber) {
  document.getElementById('suppliersModalTitle').textContent = 'Suppliers — Version v' + versionNumber;
  document.getElementById('suppliersModal').classList.add('active');
  document.getElementById('suppliersTableWrap').innerHTML = '<div class="empty">Loading...</div>';
  document.getElementById('supplierSearch').value = '';

  const resp = await fetch(BASE + '/api/versions/' + versionId + '/suppliers');
  const data = await resp.json();
  allModalSuppliers = data.suppliers || [];
  renderSupplierTable(allModalSuppliers);
};

function filterSuppliers() {
  const q = document.getElementById('supplierSearch').value.toLowerCase();
  if (!q) {
    renderSupplierTable(allModalSuppliers);
    return;
  }
  const filtered = allModalSuppliers.filter(s =>
    s.supplier_name.toLowerCase().includes(q) ||
    (s.aliases && s.aliases.toLowerCase().includes(q))
  );
  renderSupplierTable(filtered);
};

function renderSupplierTable(suppliers) {
  const wrap = document.getElementById('suppliersTableWrap');
  if (!suppliers.length) {
    wrap.innerHTML = '<div class="empty">No suppliers found.</div>';
    return;
  }

  let html = '<table><thead><tr><th style="min-width:180px">Name</th><th>Industry</th><th>Aliases</th><th>Taxonomy</th><th>Cat A</th><th style="width:60px"></th></tr></thead><tbody>';
  for (const s of suppliers) {
    let info = null;
    try { info = JSON.parse(s.supplier_info); } catch {}
    const industry = info?.industry || '<span style="color:#9ca3af">—</span>';
    const aliases = parseJSONSafe(s.aliases, []);
    const aliasText = aliases.length > 0 ? escapeHtml(aliases.join(', ')) : '<span style="color:#9ca3af">—</span>';
    const taxonomy = parseJSONSafe(s.mapped_taxonomy_paths, []);
    const taxText = taxonomy.length > 0 ? taxonomy.map(t => escapeHtml(t)).join('<br>') : '<span style="color:#9ca3af">—</span>';
    const catA = s.category_a_supplier ? '<span class="badge badge-green">Yes</span>' : '';
    const rowId = 'detail-' + s.id.replace(/[^a-zA-Z0-9]/g, '');

    html += '<tr>'
      + '<td><strong>' + escapeHtml(s.supplier_name) + '</strong></td>'
      + '<td>' + industry + '</td>'
      + '<td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + aliasText + '</td>'
      + '<td style="max-width:220px;font-size:12px">' + taxText + '</td>'
      + '<td style="text-align:center">' + catA + '</td>'
      + '<td>' + (info ? '<span class="info-toggle" onclick="toggleInfo(\\'' + rowId + '\\')">Details</span>' : '') + '</td>'
      + '</tr>';

    // Expandable detail row — show ALL fields, blank if empty
    if (info) {
      let detailHtml = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px 24px;font-size:13px;padding:8px 0">';
      const fields = [
        ['Description', info.description],
        ['Industry', info.industry],
        ['Website', info.website],
        ['Headquarters', info.headquarters],
        ['Employee Count', info.employeeCount],
        ['Revenue', info.revenue],
        ['Parent Company', info.parentCompany],
        ['Products', Array.isArray(info.products) ? info.products.join(', ') : info.products],
        ['Services', Array.isArray(info.services) ? info.services.join(', ') : info.services],
        ['Supplier Type', info.supplier_type],
        ['Confidence', info.confidence],
      ];
      for (const [label, value] of fields) {
        const display = value ? escapeHtml(String(value)) : '<span style="color:#d1d5db">—</span>';
        detailHtml += '<div><span style="color:#6b7280;font-size:11px;text-transform:uppercase">' + label + '</span><br><span>' + display + '</span></div>';
      }
      detailHtml += '</div>';

      html += '<tr id="' + rowId + '" style="display:none"><td colspan="6" style="background:#f9fafb;padding:12px 16px">' + detailHtml + '</td></tr>';
    }
  }
  html += '</tbody></table>';
  wrap.innerHTML = html;
}

function toggleInfo(id) {
  const el = document.getElementById(id);
  if (el) el.style.display = el.style.display === 'none' ? 'table-row' : 'none';
};

function closeSuppliersModal() {
  document.getElementById('suppliersModal').classList.remove('active');
};

// ─── Notes Editor ────────────────────────────────────────────────────────────

function editNotes(versionId) {
  document.getElementById('notesVersionId').value = versionId;
  document.getElementById('notesInput').value = versionNotesMap[versionId] || '';
  document.getElementById('notesModal').classList.add('active');
};

async function saveNotes() {
  const versionId = document.getElementById('notesVersionId').value;
  const notes = document.getElementById('notesInput').value.trim();

  const resp = await fetch(BASE + '/api/versions/' + versionId + '/notes', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ notes }),
  });

  if (resp.ok) {
    toast('Notes updated');
    closeNotesModal();
    loadVersions();
  } else {
    toast('Failed to save notes');
  }
};

function closeNotesModal() {
  document.getElementById('notesModal').classList.remove('active');
};

// ─── Delete Version ──────────────────────────────────────────────────────────

async function deleteVersion(versionId, versionNumber) {
  if (!confirm('Delete version v' + versionNumber + '? This will remove all supplier data for this version.')) return;

  const resp = await fetch(BASE + '/api/versions/' + versionId, { method: 'DELETE' });
  if (resp.ok) {
    toast('Version v' + versionNumber + ' deleted');
    loadVersions();
  } else {
    toast('Failed to delete version');
  }
};

// ─── Utilities ───────────────────────────────────────────────────────────────

function setProgress(pct, text) {
  document.getElementById('progressFill').style.width = pct + '%';
  document.getElementById('progressText').textContent = text;
}

function toast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.display = 'block';
  setTimeout(() => t.style.display = 'none', 3000);
}

function escapeHtml(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

function escapeAttr(str) {
  return JSON.stringify(str);
}

function parseJSONSafe(str, fallback) {
  if (!str) return fallback;
  try { return JSON.parse(str); } catch { return fallback; }
}

// ─── Diff ────────────────────────────────────────────────────────────────────

function renderDiff(diff, title) {
  document.getElementById('diffModalTitle').textContent = title || 'Version Diff';
  const wrap = document.getElementById('diffContent');

  let html = '<div class="diff-summary">'
    + '<div class="diff-stat diff-added"><div class="num">' + diff.summary.added + '</div><div class="lbl">Added</div></div>'
    + '<div class="diff-stat diff-updated"><div class="num">' + diff.summary.updated + '</div><div class="lbl">Updated</div></div>'
    + '<div class="diff-stat diff-unchanged"><div class="num">' + diff.summary.unchanged + '</div><div class="lbl">Unchanged</div></div>'
    + '</div>';

  if (diff.added.length > 0) {
    html += '<div class="diff-section"><h3>Added Suppliers (' + diff.added.length + ')</h3><ul class="diff-list">';
    for (const s of diff.added) {
      html += '<li class="add">+ ' + escapeHtml(s.supplier_name) + '</li>';
    }
    html += '</ul></div>';
  }

  if (diff.updated.length > 0) {
    html += '<div class="diff-section"><h3>Updated Suppliers (' + diff.updated.length + ')</h3><ul class="diff-list">';
    for (const s of diff.updated) {
      html += '<li class="upd">~ ' + escapeHtml(s.supplier_name) + '<span class="changes">(' + s.changes.join(', ') + ')</span></li>';
    }
    html += '</ul></div>';
  }

  if (diff.summary.added === 0 && diff.summary.updated === 0) {
    html += '<div class="empty">No changes between these versions.</div>';
  }

  wrap.innerHTML = html;
  document.getElementById('diffModal').classList.add('active');
}

function closeDiffModal() {
  document.getElementById('diffModal').classList.remove('active');
};

// Show diff after sync
function showSyncDiff(result) {
  if (result.diff) {
    renderDiff(result.diff, 'Sync Result — v' + (result.versionNumber - 1) + ' → v' + result.versionNumber);
  }
}

// Compare two versions
async function compareVersions() {
  const fromId = document.getElementById('compareFrom').value;
  const toId = document.getElementById('compareTo').value;
  if (!fromId || !toId) { toast('Select two versions'); return; }
  if (fromId === toId) { toast('Select different versions'); return; }

  const resp = await fetch(BASE + '/api/diff?from=' + fromId + '&to=' + toId);
  const data = await resp.json();
  if (data.error) { toast(data.error); return; }

  renderDiff(data.diff, 'v' + data.from.version_number + ' → v' + data.to.version_number);
};

// Populate compare dropdowns when versions load
function updateCompareDropdowns(versions) {
  const card = document.getElementById('compareCard');
  if (versions.length < 2) { card.style.display = 'none'; return; }
  card.style.display = 'block';

  const fromSel = document.getElementById('compareFrom');
  const toSel = document.getElementById('compareTo');
  let opts = '';
  for (const v of versions) {
    opts += '<option value="' + v.id + '">v' + v.version_number + ' (' + new Date(v.created_at + "Z").toLocaleDateString() + ')</option>';
  }
  fromSel.innerHTML = opts;
  toSel.innerHTML = opts;

  // Default: compare second-latest vs latest
  if (versions.length >= 2) {
    fromSel.value = versions[1].id;
    toSel.value = versions[0].id;
  }
}

// ─── Expose helpers for module script ────────────────────────────────────────
window.setProgress = setProgress;
window.toast = toast;
window.showSyncDiff = showSyncDiff;
window.parsedSuppliers = parsedSuppliers;

// ─── Init ────────────────────────────────────────────────────────────────────
loadVersions();
</script>

<script type="module">
// ─── DuckDB-WASM (isolated module — does not block UI) ─────────────────────

import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';

let duckDB = null;
let duckConn = null;

async function initDuckDB() {
  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

  const workerScript = await fetch(bundle.mainWorker).then(r => r.text());
  const workerBlob = new Blob([workerScript], { type: 'application/javascript' });
  const workerUrl = URL.createObjectURL(workerBlob);
  const worker = new Worker(workerUrl);

  const logger = new duckdb.ConsoleLogger();
  duckDB = new duckdb.AsyncDuckDB(logger, worker);
  await duckDB.instantiate(bundle.mainModule, bundle.pthreadWorker);
}

async function parseDBFile(file) {
  if (!duckDB) await initDuckDB();

  const arrayBuf = await file.arrayBuffer();
  await duckDB.registerFileBuffer('universe.db', new Uint8Array(arrayBuf));

  duckConn = await duckDB.connect();
  await duckConn.query("ATTACH 'universe.db' AS uploaded (READ_ONLY)");

  let prefix = 'uploaded.';
  try {
    await duckConn.query('SELECT COUNT(*) FROM uploaded.suppliers');
  } catch (e1) {
    console.warn('uploaded.suppliers not found, trying without prefix:', e1.message);
    try {
      await duckConn.query('SELECT COUNT(*) FROM suppliers');
      prefix = '';
    } catch (e2) {
      console.error('suppliers also not found:', e2.message);
      // List all available tables for debugging
      try {
        const allTables = await duckConn.query("SELECT table_catalog, table_schema, table_name FROM information_schema.tables");
        console.log('Available tables:', allTables.toArray().map(r => r.table_catalog + '.' + r.table_schema + '.' + r.table_name));
      } catch {}
      throw new Error('No "suppliers" table found in the database file');
    }
  }

  console.log('Using prefix:', JSON.stringify(prefix));
  const result = await duckConn.query(
    'SELECT id, supplier_name, aliases, parent_supplier_id, parent_name, supplier_info, mapped_taxonomy_paths, category_a_supplier, created_at, updated_at FROM ' + prefix + 'suppliers ORDER BY supplier_name'
  );

  const rows = result.toArray().map(row => ({
    id: row.id,
    supplier_name: row.supplier_name,
    aliases: row.aliases,
    parent_supplier_id: row.parent_supplier_id || null,
    parent_name: row.parent_name || null,
    supplier_info: row.supplier_info,
    mapped_taxonomy_paths: row.mapped_taxonomy_paths,
    category_a_supplier: row.category_a_supplier ? 1 : 0,
    created_at: row.created_at ? String(row.created_at) : null,
    updated_at: row.updated_at ? String(row.updated_at) : null,
  }));

  try { await duckConn.query("DETACH uploaded"); } catch {}
  await duckConn.close();
  duckConn = null;

  return rows;
}

// ─── Upload Handling ─────────────────────────────────────────────────────────

const dropZone = document.getElementById('dropZone');
const fileInput = document.getElementById('fileInput');

dropZone.addEventListener('dragover', (e) => {
  e.preventDefault();
  dropZone.classList.add('dragover');
});

dropZone.addEventListener('dragleave', () => {
  dropZone.classList.remove('dragover');
});

dropZone.addEventListener('drop', (e) => {
  e.preventDefault();
  dropZone.classList.remove('dragover');
  const file = e.dataTransfer.files[0];
  if (file) handleFile(file);
});

fileInput.addEventListener('change', (e) => {
  const file = e.target.files[0];
  if (file) handleFile(file);
});

async function handleFile(file) {
  document.getElementById('fileName').textContent = file.name;
  document.getElementById('preview').style.display = 'none';
  document.getElementById('syncBtn').disabled = true;

  var progressWrap = document.getElementById('progressWrap');
  progressWrap.style.display = 'block';
  window.setProgress(0, 'Reading database file...');

  try {
    window.setProgress(30, 'Parsing DuckDB file with WASM...');
    var suppliers = await parseDBFile(file);
    window.parsedSuppliers = suppliers;
    window.setProgress(100, 'Done!');

    document.getElementById('supplierCount').textContent = suppliers.length.toLocaleString();
    document.getElementById('preview').style.display = 'block';
    document.getElementById('syncBtn').disabled = false;

    setTimeout(function() { progressWrap.style.display = 'none'; }, 1000);
  } catch (err) {
    window.setProgress(0, 'Error: ' + err.message);
    window.toast('Failed to parse database: ' + err.message);
    console.error(err);
  }
}
</script>
</body>
</html>`;

// ─── Router ─────────────────────────────────────────────────────────────────

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const { pathname } = url;

    try {
      // CORS preflight
      if (request.method === 'OPTIONS') {
        return jsonResponse(null, 204);
      }

      // Initialize schema on first request
      await initializeSchema(env.DB);

      // Dashboard
      if (request.method === 'GET' && pathname === '/') {
        return new Response(DASHBOARD_HTML, {
          headers: { 'Content-Type': 'text/html; charset=utf-8' },
        });
      }

      // API routes
      if (request.method === 'POST' && pathname === '/api/sync') {
        return handleSync(request, env.DB);
      }

      if (request.method === 'GET' && pathname === '/api/versions') {
        return handleGetVersions(env.DB);
      }

      if (request.method === 'GET' && pathname === '/api/diff') {
        return handleDiff(url, env.DB);
      }

      // /api/versions/:id/notes
      const notesMatch = pathname.match(/^\/api\/versions\/(\d+)\/notes$/);
      if (request.method === 'PUT' && notesMatch) {
        return handleUpdateNotes(notesMatch[1], request, env.DB);
      }

      // /api/versions/:id/suppliers
      const suppliersMatch = pathname.match(/^\/api\/versions\/(\d+)\/suppliers$/);
      if (request.method === 'GET' && suppliersMatch) {
        return handleGetSuppliers(suppliersMatch[1], env.DB);
      }

      // /api/versions/:id
      const deleteMatch = pathname.match(/^\/api\/versions\/(\d+)$/);
      if (request.method === 'DELETE' && deleteMatch) {
        return handleDeleteVersion(deleteMatch[1], env.DB);
      }

      return jsonResponse({ error: 'Not found' }, 404);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error('[fetch] error:', message);
      return jsonResponse({ error: 'Internal server error', detail: message }, 500);
    }
  },
};
