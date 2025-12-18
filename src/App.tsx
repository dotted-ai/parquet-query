import { useMemo, useRef, useState, type ChangeEvent, useEffect } from 'react';
import CodeMirror, { type ReactCodeMirrorRef } from '@uiw/react-codemirror';
import { sql as sqlLanguage } from '@codemirror/lang-sql';
import { EditorView, keymap } from '@codemirror/view';
import { defaultKeymap } from '@codemirror/commands';
import { recordBatchesToCSVParts, tableToRows } from './arrow';
import {
  collectFilesFromDirectoryHandle,
  collectFilesFromFileList,
  type ImportedFile,
} from './fileImport';
import { exec, getDuckDB, query, registerFileBuffer, send } from './duckdb';

const DEFAULT_SQL = `-- Dica: você pode consultar arquivos diretamente pelo caminho registrado.
-- Exemplos:
--   SELECT * FROM 'meus_dados/arquivo.parquet' LIMIT 50;
--   SELECT COUNT(*) FROM 'meus_dados/arquivo.csv';
-- Se você der um nome para a tabela no import, pode consultar tudo de uma vez:
--   SELECT * FROM minha_tabela LIMIT 50;

SELECT 42 AS ok;`;

interface QueryTab {
  id: string;
  name: string;
  sql: string;
  isDirty: boolean;
  category: 'scripts' | 'bookmarks' | 'templates';
}

const STORAGE_KEY = 'parquet-query-tabs';

function bytes(size: number) {
  const units = ['B', 'KB', 'MB', 'GB'];
  let idx = 0;
  let value = size;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx++;
  }
  return `${value.toFixed(idx === 0 ? 0 : 1)} ${units[idx]}`;
}

function sqlStringLiteral(value: string) {
  return `'${value.replaceAll("'", "''")}'`;
}

function sqlIdentifier(value: string) {
  const trimmed = value.trim();
  if (!trimmed) throw new Error('Nome da tabela vazio');
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(trimmed)) return trimmed;
  return `"${trimmed.replaceAll('"', '""')}"`;
}

function parquetPaths(imported: ImportedFile[]) {
  return imported
    .filter((f) => f.path.toLowerCase().endsWith('.parquet'))
    .map((f) => f.path);
}

function tableExampleSQL(tableName: string) {
  const ident = sqlIdentifier(tableName);
  return `-- Exemplo rápido\nSELECT * FROM ${ident} LIMIT 50;`;
}

function statementAtPosition(sql: string, position: number) {
  type Segment = { start: number; end: number };
  const segments: Segment[] = [];

  let start = 0;
  let inSingle = false;
  let inDouble = false;
  let inLineComment = false;
  let inBlockComment = false;

  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i]!;
    const next = i + 1 < sql.length ? sql[i + 1]! : '';

    if (inLineComment) {
      if (ch === '\n') inLineComment = false;
      continue;
    }

    if (inBlockComment) {
      if (ch === '*' && next === '/') {
        inBlockComment = false;
        i++;
      }
      continue;
    }

    if (inSingle) {
      if (ch === "'" && next === "'") {
        i++;
        continue;
      }
      if (ch === "'") inSingle = false;
      continue;
    }

    if (inDouble) {
      if (ch === '"' && next === '"') {
        i++;
        continue;
      }
      if (ch === '"') inDouble = false;
      continue;
    }

    if (ch === '-' && next === '-') {
      inLineComment = true;
      i++;
      continue;
    }
    if (ch === '/' && next === '*') {
      inBlockComment = true;
      i++;
      continue;
    }
    if (ch === "'") {
      inSingle = true;
      continue;
    }
    if (ch === '"') {
      inDouble = true;
      continue;
    }
    if (ch === ';') {
      segments.push({ start, end: i });
      start = i + 1;
    }
  }
  segments.push({ start, end: sql.length });

  const clampedPos = Math.max(0, Math.min(position, sql.length));
  let idx = segments.findIndex((s) => clampedPos >= s.start && clampedPos <= s.end);
  if (idx === -1) idx = segments.length - 1;

  const pick = (i: number) => sql.slice(segments[i]!.start, segments[i]!.end).trim();
  if (pick(idx)) return pick(idx);
  for (let j = idx - 1; j >= 0; j--) {
    const v = pick(j);
    if (v) return v;
  }
  for (let j = idx + 1; j < segments.length; j++) {
    const v = pick(j);
    if (v) return v;
  }
  return '';
}

const TEMPLATE_QUERIES = {
  'CUR - Resumo por Conta': `-- Resumo de custos por conta
SELECT
  line_item_usage_account_id AS account_id,
  COUNT(*) AS total_linhas,
  round(SUM(line_item_unblended_cost), 2) AS total_custo,
  MIN(bill_billing_period_start_date) AS periodo_inicio,
  MAX(bill_billing_period_start_date) AS periodo_fim
FROM dotted_org_cur
WHERE bill_billing_period_start_date >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY line_item_usage_account_id
ORDER BY total_custo DESC;`,

  'CUR - Resumo por Serviço': `-- Resumo de custos por serviço
SELECT
  COALESCE(product_servicename, product_product_name, line_item_line_item_type) AS service_name,
  COUNT(*) AS total_linhas,
  round(SUM(line_item_unblended_cost), 2) AS total_custo
FROM dotted_org_cur
WHERE bill_billing_period_start_date >= CURRENT_DATE - INTERVAL '30' DAY
  AND line_item_line_item_type <> 'Tax'
GROUP BY service_name
ORDER BY total_custo DESC
LIMIT 50;`,

  'CUR - Resumo por Mês': `-- Resumo de custos por mês
SELECT
  date_trunc('month', bill_billing_period_start_date) AS mes,
  COUNT(*) AS total_linhas,
  round(SUM(line_item_unblended_cost), 2) AS total_custo
FROM dotted_org_cur
WHERE bill_billing_period_start_date >= CURRENT_DATE - INTERVAL '90' DAY
  AND line_item_line_item_type <> 'Tax'
GROUP BY mes
ORDER BY mes DESC;`,

  'CUR - Detalhes por Conta': `-- Detalhes de custos para uma conta específica
SELECT
  line_item_usage_account_id AS account_id,
  COALESCE(product_servicename, product_product_name, line_item_line_item_type) AS service_name,
  date_trunc('month', bill_billing_period_start_date) AS mes,
  round(SUM(line_item_unblended_cost), 2) AS custo
FROM dotted_org_cur
WHERE line_item_usage_account_id = '331957531828'  -- Ajuste o account_id
  AND bill_billing_period_start_date BETWEEN TIMESTAMP '2025-12-01' AND TIMESTAMP '2025-12-31'
  AND line_item_line_item_type <> 'Tax'
GROUP BY account_id, service_name, mes
ORDER BY mes DESC, custo DESC;`,

  'CUR - Savings Plans': `-- Análise de Savings Plans
SELECT
  line_item_usage_account_id AS account_id,
  date_trunc('month', bill_billing_period_start_date) AS mes,
  round(SUM(savings_plan_total_commitment_to_date), 2) AS sp_commitment,
  round(SUM(savings_plan_savings_plan_effective_cost), 2) AS sp_effective_cost,
  round(SUM(CASE WHEN line_item_line_item_type = 'SavingsPlanCoveredUsage' THEN line_item_unblended_cost ELSE 0 END), 2) AS sp_usage_save
FROM dotted_org_cur
WHERE bill_billing_period_start_date >= CURRENT_DATE - INTERVAL '90' DAY
  AND savings_plan_savings_plan_a_r_n IS NOT NULL
GROUP BY account_id, mes
ORDER BY mes DESC, account_id;`,

  'CUR - Reserved Instances': `-- Análise de Reserved Instances
SELECT
  line_item_usage_account_id AS account_id,
  date_trunc('month', bill_billing_period_start_date) AS mes,
  round(SUM(CASE WHEN line_item_line_item_type = 'RIFee' THEN line_item_unblended_cost ELSE 0 END), 2) AS ri_cost,
  round(SUM(CASE WHEN line_item_line_item_type = 'DiscountedUsage' THEN reservation_effective_cost ELSE 0 END), 2) AS ri_usage_cost,
  round(SUM(reservation_unused_recurring_fee), 2) AS unused_ri
FROM dotted_org_cur
WHERE bill_billing_period_start_date >= CURRENT_DATE - INTERVAL '90' DAY
  AND reservation_reservation_a_r_n IS NOT NULL
GROUP BY account_id, mes
ORDER BY mes DESC, account_id;`,
};

function loadTabsFromStorage(): QueryTab[] {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored) {
      return JSON.parse(stored);
    }
  } catch (e) {
    console.warn('Erro ao carregar tabs do localStorage', e);
  }
  return [
    {
      id: '1',
      name: 'Script-1',
      sql: DEFAULT_SQL,
      isDirty: false,
      category: 'scripts',
    },
  ];
}

function saveTabsToStorage(tabs: QueryTab[]) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(tabs));
  } catch (e) {
    console.warn('Erro ao salvar tabs no localStorage', e);
  }
}

export default function App() {
  const [dbStatus, setDbStatus] = useState<'idle' | 'loading' | 'ready'>('idle');
  const [files, setFiles] = useState<ImportedFile[]>([]);
  const [folderName, setFolderName] = useState<string>('');
  const [parquetTableName, setParquetTableName] = useState<string>('');
  const [tabs, setTabs] = useState<QueryTab[]>(loadTabsFromStorage);
  const [activeTabId, setActiveTabId] = useState<string>(tabs[0]?.id || '1');
  const [renamingTabId, setRenamingTabId] = useState<string | null>(null);
  const [renameDraft, setRenameDraft] = useState<string>('');
  const [running, setRunning] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [error, setError] = useState<string>('');
  const [resultInfo, setResultInfo] = useState<string>('');
  const [importInfo, setImportInfo] = useState<string>('');
  const [table, setTable] = useState<{ columns: string[]; rows: string[][] }>();
  const [sort, setSort] = useState<{ col: number; dir: 'asc' | 'desc' } | null>(null);
  const [rowSearch, setRowSearch] = useState<string>('');
  const [loadingFiles, setLoadingFiles] = useState(false);
  const [sidebarExpanded, setSidebarExpanded] = useState(true);
  const [selectedCategory, setSelectedCategory] = useState<'scripts' | 'bookmarks' | 'templates'>('scripts');

  const codeMirrorRef = useRef<ReactCodeMirrorRef | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const activeTab = useMemo(() => tabs.find((t) => t.id === activeTabId) || tabs[0], [tabs, activeTabId]);
  const currentSQL = activeTab?.sql || DEFAULT_SQL;

  useEffect(() => {
    saveTabsToStorage(tabs);
  }, [tabs]);

  const supportsDirectoryPicker = useMemo(
    () => typeof (window as any).showDirectoryPicker === 'function',
    [],
  );

  function getSQLAtCursorOrSelection() {
    const view = codeMirrorRef.current?.view;
    if (!view) return currentSQL;
    const sel = view.state.selection.main;
    const selected = sel.from !== sel.to ? view.state.sliceDoc(sel.from, sel.to).trim() : '';
    if (selected) return selected;
    const doc = view.state.doc.toString();
    const stmt = statementAtPosition(doc, sel.head).trim();
    return stmt || doc.trim();
  }

  async function ensureDbReady() {
    if (dbStatus === 'ready' || dbStatus === 'loading') return;
    setDbStatus('loading');
    try {
      await getDuckDB();
      setDbStatus('ready');
    } catch (e) {
      setDbStatus('idle');
      throw e;
    }
  }

  async function createOrReplaceParquetView(imported: ImportedFile[], tableName: string) {
    const name = tableName.trim();
    if (!name) return;
    const paths = parquetPaths(imported);
    if (paths.length === 0) {
      throw new Error('Nenhum arquivo .parquet encontrado para criar a tabela.');
    }
    const ident = sqlIdentifier(name);
    const list = `[${paths.map(sqlStringLiteral).join(', ')}]`;
    await exec(`CREATE OR REPLACE VIEW ${ident} AS SELECT * FROM read_parquet(${list});`);
    setImportInfo(`Tabela ${name} criada com ${paths.length} parquet(s).`);
  }

  async function importFiles(selectedFiles: File[], meta: ImportedFile[], label: string) {
    setError('');
    setResultInfo('');
    setImportInfo('');
    setTable(undefined);
    setLoadingFiles(true);

    try {
      await ensureDbReady();

      const imported: ImportedFile[] = [];
      for (let i = 0; i < selectedFiles.length; i++) {
        const file = selectedFiles[i]!;
        const fileMeta = meta[i]!;
        const data = await file.arrayBuffer();
        await registerFileBuffer(fileMeta.path, data);
        imported.push(fileMeta);
      }

      setFolderName(label);
      setFiles(imported);
      await createOrReplaceParquetView(imported, parquetTableName);
    } finally {
      setLoadingFiles(false);
    }
  }

  async function onPickFolder() {
    setError('');
    if (!supportsDirectoryPicker) {
      fileInputRef.current?.click();
      return;
    }

    try {
      const dirHandle = await (window as any).showDirectoryPicker();
      const rootPrefix = dirHandle?.name ? String(dirHandle.name) : '';
      const collected = await collectFilesFromDirectoryHandle(dirHandle, rootPrefix);
      await importFiles(collected.files, collected.meta, dirHandle?.name || 'pasta');
    } catch (e: any) {
      if (e?.name === 'AbortError') return;
      setError(e?.message || String(e));
    }
  }

  async function onFolderInputChange(e: ChangeEvent<HTMLInputElement>) {
    const list = e.target.files;
    e.target.value = '';
    if (!list || list.length === 0) return;
    try {
      const collected = collectFilesFromFileList(list);
      const label = collected.meta[0]?.path?.split('/')?.[0] || 'pasta selecionada';
      await importFiles(collected.files, collected.meta, label);
    } catch (err: any) {
      setError(err?.message || String(err));
    }
  }

  function createNewTab(category: 'scripts' | 'bookmarks' | 'templates' = 'scripts') {
    const newId = String(Date.now());
    const newTab: QueryTab = {
      id: newId,
      name: `Script-${tabs.filter((t) => t.category === category).length + 1}`,
      sql: DEFAULT_SQL,
      isDirty: false,
      category,
    };
    setTabs([...tabs, newTab]);
    setActiveTabId(newId);
    setSelectedCategory(category);
  }

  function closeTab(tabId: string) {
    if (tabs.length === 1) return;
    const newTabs = tabs.filter((t) => t.id !== tabId);
    setTabs(newTabs);
    if (activeTabId === tabId) {
      setActiveTabId(newTabs[0]?.id || '1');
    }
  }

  function updateTabSQL(tabId: string, sql: string) {
    setTabs((prev) =>
      prev.map((t) => (t.id === tabId ? { ...t, sql, isDirty: true } : t)),
    );
  }

  function updateTabName(tabId: string, name: string) {
    setTabs((prev) => prev.map((t) => (t.id === tabId ? { ...t, name: name.trim() || t.name } : t)));
  }

  function markTabClean(tabId: string) {
    setTabs((prev) => prev.map((t) => (t.id === tabId ? { ...t, isDirty: false } : t)));
  }

  async function runQuery(sqlToRun?: string) {
    setError('');
    setRunning(true);
    setResultInfo('');
    setTable(undefined);
    setSort(null);
    setRowSearch('');
    try {
      await ensureDbReady();
      const statement = (sqlToRun ?? currentSQL).trim();
      const result = await query(statement);
      const rows = tableToRows(result, 200);
      setTable(rows);
      setResultInfo(
        `Linhas: ${result.numRows.toLocaleString()} (mostrando ${rows.rows.length}) · Colunas: ${rows.columns.length}`,
      );
      markTabClean(activeTabId);
    } catch (e: any) {
      setError(e?.message || String(e));
    } finally {
      setRunning(false);
    }
  }

  function toggleSort(col: number) {
    setSort((prev) => {
      if (!prev || prev.col !== col) return { col, dir: 'asc' };
      if (prev.dir === 'asc') return { col, dir: 'desc' };
      return null;
    });
  }

  function compareCells(aRaw: string, bRaw: string) {
    const a = aRaw?.trim?.() ?? '';
    const b = bRaw?.trim?.() ?? '';
    const aEmpty = a === '';
    const bEmpty = b === '';
    if (aEmpty && bEmpty) return 0;
    if (aEmpty) return 1;
    if (bEmpty) return -1;

    if (/^-?\d+(\.\d+)?$/.test(a) && /^-?\d+(\.\d+)?$/.test(b)) {
      const an = Number(a);
      const bn = Number(b);
      if (Number.isFinite(an) && Number.isFinite(bn)) return an - bn;
    }

    const aHasDateHint = a.includes('-') || a.includes('T') || a.includes(':');
    const bHasDateHint = b.includes('-') || b.includes('T') || b.includes(':');
    if (aHasDateHint && bHasDateHint) {
      const at = Date.parse(a);
      const bt = Date.parse(b);
      if (!Number.isNaN(at) && !Number.isNaN(bt)) return at - bt;
    }

    return a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' });
  }

  const filteredTable = useMemo(() => {
    if (!table) return table;
    const q = rowSearch.trim().toLowerCase();
    if (!q) return table;
    return {
      columns: table.columns,
      rows: table.rows.filter((r) => r.some((cell) => (cell ?? '').toLowerCase().includes(q))),
    };
  }, [table, rowSearch]);

  const sortedTable = useMemo(() => {
    if (!filteredTable || !sort) return filteredTable;
    const dir = sort.dir === 'asc' ? 1 : -1;
    const rowsWithIdx = filteredTable.rows.map((row, idx) => ({ row, idx }));
    rowsWithIdx.sort((ra, rb) => {
      const cmp = compareCells(ra.row[sort.col] ?? '', rb.row[sort.col] ?? '');
      if (cmp !== 0) return cmp * dir;
      return ra.idx - rb.idx;
    });
    return { columns: filteredTable.columns, rows: rowsWithIdx.map((r) => r.row) };
  }, [filteredTable, sort]);

  async function exportCSV() {
    setError('');
    setExporting(true);
    try {
      await ensureDbReady();
      const statement = getSQLAtCursorOrSelection();
      const batches = await send(statement);
      const { parts, rows } = await recordBatchesToCSVParts(batches);
      const blob = new Blob(parts, { type: 'text/csv;charset=utf-8' });
      const fileName = `query-${new Date().toISOString().replaceAll(':', '-')}.csv`;
      const url = URL.createObjectURL(blob);
      try {
        const a = document.createElement('a');
        a.href = url;
        a.download = fileName;
        document.body.appendChild(a);
        a.click();
        a.remove();
      } finally {
        URL.revokeObjectURL(url);
      }
      setResultInfo(`CSV exportado: ${rows.toLocaleString()} linhas`);
    } catch (e: any) {
      setError(e?.message || String(e));
    } finally {
      setExporting(false);
    }
  }

  async function onCreateTable() {
    setError('');
    setImportInfo('');
    try {
      await ensureDbReady();
      await createOrReplaceParquetView(files, parquetTableName);
    } catch (e: any) {
      setError(e?.message || String(e));
    }
  }

  const runQueryRef = useRef(runQuery);
  runQueryRef.current = runQuery;

  const executeQueryKeymap = useMemo(() => {
    return keymap.of([
      {
        key: 'Mod-Enter',
        run: () => {
          if (!running && dbStatus === 'ready') {
            runQueryRef.current(getSQLAtCursorOrSelection());
          }
          return true;
        },
      },
    ]);
  }, [running, dbStatus]);

  const visibleTabs = useMemo(() => tabs.filter((t) => t.category === selectedCategory), [tabs, selectedCategory]);

  function startRenameTab(tab: QueryTab) {
    setRenamingTabId(tab.id);
    setRenameDraft(tab.name);
  }

  function commitRenameTab() {
    if (!renamingTabId) return;
    updateTabName(renamingTabId, renameDraft);
    setRenamingTabId(null);
    setRenameDraft('');
  }

  function cancelRenameTab() {
    setRenamingTabId(null);
    setRenameDraft('');
  }

  return (
    <div className="container">
      <div className="header">
        <h1 className="title">🚀 Parquet Query</h1>
        <div className="muted">
          {dbStatus === 'ready' ? (
            <span className="pill ok">✓ DuckDB pronto</span>
          ) : dbStatus === 'loading' ? (
            <span className="pill">⏳ Carregando DuckDB…</span>
          ) : (
            <span className="pill">○ DuckDB não inicializado</span>
          )}
        </div>
      </div>

      <div className="main-layout">
        <div className={`sidebar ${sidebarExpanded ? 'expanded' : 'collapsed'}`}>
          <div className="sidebar-header">
            <button
              className="sidebar-toggle"
              onClick={() => setSidebarExpanded(!sidebarExpanded)}
              title={sidebarExpanded ? 'Recolher sidebar' : 'Expandir sidebar'}
            >
              {sidebarExpanded ? '◀' : '▶'}
            </button>
            {sidebarExpanded && <h3>Navegação</h3>}
          </div>

          {sidebarExpanded && (
            <>
              <div className="sidebar-section">
                <div className="sidebar-category">
                  <button
                    className={`category-btn ${selectedCategory === 'scripts' ? 'active' : ''}`}
                    onClick={() => setSelectedCategory('scripts')}
                  >
                    📝 Scripts
                  </button>
                  <button
                    className={`category-btn ${selectedCategory === 'bookmarks' ? 'active' : ''}`}
                    onClick={() => setSelectedCategory('bookmarks')}
                  >
                    🔖 Bookmarks
                  </button>
                  <button
                    className={`category-btn ${selectedCategory === 'templates' ? 'active' : ''}`}
                    onClick={() => setSelectedCategory('templates')}
                  >
                    📋 Templates
                  </button>
                </div>
              </div>

              <div className="sidebar-section">
                <div className="sidebar-files">
                  <div className="sidebar-files-header">
                    <span>Arquivos</span>
                    <button
                      className="icon-btn"
                      onClick={() => createNewTab(selectedCategory)}
                      title="Novo script"
                    >
                      +
                    </button>
                  </div>
                  <div className="file-list">
                    {visibleTabs.map((tab) => (
                      <div
                        key={tab.id}
                        className={`file-item ${activeTabId === tab.id ? 'active' : ''}`}
                        onClick={() => setActiveTabId(tab.id)}
                        title={tab.isDirty ? `${tab.name} (modificado)` : tab.name}
                      >
                        <span className="file-name">
                          {tab.isDirty && '*'}
                          {tab.name}
                        </span>
                        <button
                          className="file-close"
                          onClick={(e) => {
                            e.stopPropagation();
                            closeTab(tab.id);
                          }}
                          title="Fechar"
                        >
                          ×
                        </button>
                      </div>
                    ))}
                  </div>
                  {selectedCategory === 'templates' && (
                    <div className="templates-list">
                      {Object.entries(TEMPLATE_QUERIES).map(([name, sql]) => (
                        <button
                          key={name}
                          className="template-btn"
                          onClick={() => {
                            const newId = String(Date.now());
                            const newTab: QueryTab = {
                              id: newId,
                              name,
                              sql,
                              isDirty: false,
                              category: 'scripts',
                            };
                            setTabs([...tabs, newTab]);
                            setActiveTabId(newId);
                            setSelectedCategory('scripts');
                          }}
                          title={`Criar script a partir do template: ${name}`}
                        >
                          {name}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              </div>

              <div className="sidebar-section">
                <h4>📁 Arquivos Importados</h4>
                <div className="row">
                  <button onClick={onPickFolder} disabled={dbStatus === 'loading' || loadingFiles} className="small-btn">
                    {supportsDirectoryPicker ? '📂 Selecionar pasta' : '📂 Selecionar pasta (fallback)'}
                  </button>
                </div>
                <input
                  type="text"
                  className="table-name-input small"
                  value={parquetTableName}
                  onChange={(e) => setParquetTableName(e.target.value)}
                  placeholder="Nome da tabela (opcional)"
                />
                <button
                  className="secondary small-btn"
                  onClick={onCreateTable}
                  disabled={dbStatus !== 'ready' || files.length === 0}
                  title="Cria/atualiza uma VIEW com todos os arquivos .parquet importados"
                >
                  ✨ Criar tabela
                </button>
                {folderName ? <span className="pill small">{folderName}</span> : null}
                {files.length ? <span className="pill small">{files.length} arquivos</span> : null}
                {importInfo ? <span className="pill ok small">{importInfo}</span> : null}

                <div className="filelist-sidebar">
                  {loadingFiles ? (
                    <div className="loader-container">
                      <div className="loader"></div>
                    </div>
                  ) : files.length === 0 ? (
                    <div className="muted" style={{ textAlign: 'center', padding: '12px', fontSize: '12px' }}>
                      📄 Nenhum arquivo
                    </div>
                  ) : (
                    files.slice(0, 10).map((f) => (
                      <div className="file-sidebar" key={f.path} title={f.path}>
                        <div>{f.path}</div>
                        <div className="muted">{bytes(f.size)}</div>
                      </div>
                    ))
                  )}
                </div>
              </div>
            </>
          )}
        </div>

        <div className="main-content">
          <input
            ref={fileInputRef}
            type="file"
            multiple
            // @ts-expect-error: atributo não padronizado, mas amplamente suportado
            webkitdirectory="true"
            onChange={onFolderInputChange}
            style={{ display: 'none' }}
          />

          <div className="card">
            <div className="tabs-container">
              <div className="tabs">
                {tabs
                  .filter((t) => t.category === selectedCategory)
                  .map((tab) => (
                    <div
                      key={tab.id}
                      className={`tab ${activeTabId === tab.id ? 'active' : ''}`}
                      onClick={() => setActiveTabId(tab.id)}
                      onDoubleClick={() => startRenameTab(tab)}
                      title="Duplo clique para renomear"
                    >
                      {renamingTabId === tab.id ? (
                        <input
                          type="text"
                          value={renameDraft}
                          onChange={(e) => setRenameDraft(e.target.value)}
                          onClick={(e) => e.stopPropagation()}
                          onDoubleClick={(e) => e.stopPropagation()}
                          autoFocus
                          style={{ minWidth: 120, fontSize: '13px' }}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') commitRenameTab();
                            if (e.key === 'Escape') cancelRenameTab();
                          }}
                          onBlur={commitRenameTab}
                        />
                      ) : (
                        <span className="tab-name">
                          {tab.isDirty && '*'}
                          {tab.name}
                        </span>
                      )}
                      <button
                        className="tab-close"
                        onClick={(e) => {
                          e.stopPropagation();
                          closeTab(tab.id);
                        }}
                        title="Fechar"
                      >
                        ×
                      </button>
                    </div>
                  ))}
                <button className="tab-new" onClick={() => createNewTab(selectedCategory)} title="Novo script">
                  +
                </button>
              </div>
            </div>

            <div className="sql-editor-wrapper">
              <CodeMirror
                ref={codeMirrorRef}
                theme="dark"
                height="55vh"
                minHeight="260px"
                maxHeight="640px"
                value={currentSQL}
                onChange={(value) => updateTabSQL(activeTabId, value)}
                extensions={[
                  sqlLanguage(),
                  executeQueryKeymap,
                  EditorView.theme({
                    '&': {
                      backgroundColor: '#1e1e1e',
                      color: '#f1f5f9',
                      fontSize: '14px',
                      borderRadius: '12px',
                      border: '1px solid rgba(148, 163, 184, 0.2)',
                      transition: 'all 0.2s ease',
                    },
                    '&.cm-focused': {
                      outline: 'none',
                      border: '1px solid #6366f1',
                      boxShadow: '0 0 0 3px rgba(99, 102, 241, 0.1), 0 4px 6px -1px rgba(0, 0, 0, 0.4), 0 2px 4px -1px rgba(0, 0, 0, 0.3)',
                      backgroundColor: '#1e1e1e',
                    },
                    '.cm-content': {
                      padding: '16px',
                      minHeight: '200px',
                      fontFamily: "'SF Mono', 'Monaco', 'Inconsolata', 'Roboto Mono', 'Source Code Pro', ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace",
                      lineHeight: '1.6',
                      color: '#f1f5f9 !important',
                    },
                    '.cm-scroller': {
                      fontFamily: "'SF Mono', 'Monaco', 'Inconsolata', 'Roboto Mono', 'Source Code Pro', ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace",
                    },
                    '.cm-editor': {
                      color: '#f1f5f9',
                    },
                    '.cm-keyword': {
                      color: '#818cf8 !important',
                      fontWeight: '600',
                      opacity: '1 !important',
                    },
                    '.cm-string': {
                      color: '#34d399 !important',
                      opacity: '1 !important',
                    },
                    '.cm-number': {
                      color: '#fbbf24 !important',
                      opacity: '1 !important',
                    },
                    '.cm-comment': {
                      color: '#94a3b8',
                      fontStyle: 'italic',
                      opacity: '1 !important',
                    },
                    '.cm-operator': {
                      color: '#c084fc !important',
                      opacity: '1 !important',
                    },
                    '.cm-builtin': {
                      color: '#60a5fa !important',
                      opacity: '1 !important',
                    },
                    '.cm-variable': {
                      color: '#f1f5f9 !important',
                      opacity: '1 !important',
                    },
                    '.cm-attribute': {
                      color: '#a78bfa !important',
                      opacity: '1 !important',
                    },
                    '.cm-line': {
                      color: '#f1f5f9 !important',
                      opacity: '1 !important',
                    },
                    '.cm-text': {
                      color: '#f1f5f9 !important',
                      opacity: '1 !important',
                    },
                    '.cm-cursor': {
                      borderLeftColor: '#6366f1',
                    },
                    '.cm-selectionBackground': {
                      backgroundColor: 'rgba(99, 102, 241, 0.2)',
                    },
                    '&.cm-focused .cm-selectionBackground': {
                      backgroundColor: 'rgba(99, 102, 241, 0.3)',
                    },
                    '.cm-lineNumbers': {
                      color: '#94a3b8',
                      fontSize: '12px',
                    },
                    '.cm-gutterElement': {
                      padding: '0 8px 0 16px',
                    },
                  }),
                ]}
                basicSetup={{
                  lineNumbers: true,
                  foldGutter: true,
                  dropCursor: false,
                  allowMultipleSelections: false,
                }}
              />
          </div>
          <div className="row" style={{ marginTop: 10 }}>
            <button onClick={() => runQuery(getSQLAtCursorOrSelection())} disabled={running || exporting}>
              {running ? '⏳ Executando…' : '▶️ Executar'}
            </button>
            <button className="secondary" onClick={exportCSV} disabled={running || exporting}>
              {exporting ? '⏳ Exportando…' : '⬇️ Exportar CSV'}
            </button>
              <input
                value={rowSearch}
                onChange={(e) => setRowSearch(e.target.value)}
                placeholder="Buscar nas linhas…"
                disabled={!table || running || exporting}
                style={{ minWidth: 220 }}
                title="Filtra apenas as linhas exibidas (até 200)"
              />
              <input
                type="text"
                value={activeTab?.name || ''}
                onChange={(e) => updateTabName(activeTabId, e.target.value)}
                placeholder="Nome do script"
                style={{ minWidth: 150, fontSize: '13px' }}
                onBlur={() => markTabClean(activeTabId)}
              />
              <button
                className="secondary"
                onClick={() => {
                  updateTabSQL(activeTabId, DEFAULT_SQL);
                  setError('');
                  setResultInfo('');
                  setTable(undefined);
                  setSort(null);
                  setRowSearch('');
                }}
                disabled={running || exporting}
                title="Restaura o SQL padrão"
              >
                ↩️ Reset SQL
              </button>
              <button
                className="secondary"
                onClick={() => {
                  updateTabSQL(activeTabId, tableExampleSQL(parquetTableName));
                  setError('');
                }}
                disabled={running || exporting || !parquetTableName.trim()}
                title="Coloca um SELECT pronto para a tabela criada"
              >
                🧪 Exemplo tabela
              </button>
              {resultInfo ? <span className="pill ok">{resultInfo}</span> : null}
              {table && rowSearch.trim() ? (
                <span className="pill">
                  {sortedTable?.rows.length ?? 0}/{table.rows.length} linhas
                </span>
              ) : null}
            </div>

            <div className="results-area">
              {error ? (
                <div className="error" style={{ marginTop: 12 }}>
                  {error}
                </div>
              ) : null}

              {table ? (
                <div style={{ marginTop: 12 }}>
                  <div className="tableWrap">
                    <table>
                      <thead>
                        <tr>
                          {table.columns.map((c, idx) => (
                            <th
                              key={c}
                              onClick={() => toggleSort(idx)}
                              style={{ cursor: 'pointer', userSelect: 'none' }}
                              title="Clique para ordenar"
                              aria-sort={
                                sort?.col === idx ? (sort.dir === 'asc' ? 'ascending' : 'descending') : 'none'
                              }
                            >
                              {c}
                              {sort?.col === idx ? (sort.dir === 'asc' ? ' ▲' : ' ▼') : null}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {(sortedTable?.rows ?? []).map((r, idx) => (
                          <tr key={idx}>
                            {r.map((cell, cidx) => (
                              <td key={cidx}>{cell}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  <div className="muted" style={{ marginTop: 12, textAlign: 'center', fontSize: '12px' }}>
                    📊 Mostrando até 200 linhas
                  </div>
                </div>
              ) : null}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
