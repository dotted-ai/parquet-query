import { useMemo, useRef, useState, type ChangeEvent } from 'react';
import CodeMirror from '@uiw/react-codemirror';
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

export default function App() {
  const [dbStatus, setDbStatus] = useState<'idle' | 'loading' | 'ready'>('idle');
  const [files, setFiles] = useState<ImportedFile[]>([]);
  const [folderName, setFolderName] = useState<string>('');
  const [parquetTableName, setParquetTableName] = useState<string>('');
  const [sql, setSql] = useState(DEFAULT_SQL);
  const [running, setRunning] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [error, setError] = useState<string>('');
  const [resultInfo, setResultInfo] = useState<string>('');
  const [importInfo, setImportInfo] = useState<string>('');
  const [table, setTable] = useState<{ columns: string[]; rows: string[][] }>();
  const [loadingFiles, setLoadingFiles] = useState(false);

  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const supportsDirectoryPicker = useMemo(
    () => typeof (window as any).showDirectoryPicker === 'function',
    [],
  );

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

  async function runQuery() {
    setError('');
    setRunning(true);
    setResultInfo('');
    setTable(undefined);
    try {
      await ensureDbReady();
      const result = await query(sql);
      const rows = tableToRows(result, 200);
      setTable(rows);
      setResultInfo(
        `Linhas: ${result.numRows.toLocaleString()} (mostrando ${rows.rows.length}) · Colunas: ${rows.columns.length}`,
      );
    } catch (e: any) {
      setError(e?.message || String(e));
    } finally {
      setRunning(false);
    }
  }

  async function exportCSV() {
    setError('');
    setExporting(true);
    try {
      await ensureDbReady();
      const batches = await send(sql);
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
            runQueryRef.current();
          }
          return true;
        },
      },
    ]);
  }, [running, dbStatus]);

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

      <div className="grid">
        <div className="card">
          <h2>📁 Arquivos</h2>
          <div className="row">
            <button onClick={onPickFolder} disabled={dbStatus === 'loading' || loadingFiles}>
              {supportsDirectoryPicker ? '📂 Selecionar pasta' : '📂 Selecionar pasta (fallback)'}
            </button>
            <input
              type="text"
              className="table-name-input"
              value={parquetTableName}
              onChange={(e) => setParquetTableName(e.target.value)}
              placeholder="Nome da tabela (opcional)"
            />
            <button
              className="secondary"
              onClick={onCreateTable}
              disabled={dbStatus !== 'ready' || files.length === 0}
              title="Cria/atualiza uma VIEW com todos os arquivos .parquet importados"
            >
              ✨ Criar tabela
            </button>
            <button
              className="secondary"
              onClick={() => {
                setFiles([]);
                setFolderName('');
                setResultInfo('');
                setImportInfo('');
                setTable(undefined);
                setError('');
              }}
              disabled={dbStatus === 'loading' || loadingFiles}
            >
              🗑️ Limpar
            </button>
            {folderName ? <span className="pill">{folderName}</span> : null}
            {files.length ? <span className="pill">{files.length} arquivos</span> : null}
            {importInfo ? <span className="pill ok">{importInfo}</span> : null}
          </div>

          <input
            ref={fileInputRef}
            type="file"
            multiple
            // @ts-expect-error: atributo não padronizado, mas amplamente suportado
            webkitdirectory="true"
            onChange={onFolderInputChange}
            style={{ display: 'none' }}
          />

          <div className="muted" style={{ marginTop: 16, padding: '12px 16px', background: 'rgba(15, 23, 42, 0.4)', borderRadius: '10px', border: '1px solid rgba(148, 163, 184, 0.2)' }}>
            <strong>💡 Dica:</strong> Suporta parquet, csv, json, ndjson. Após importar, consulte com{' '}
            <code style={{ background: 'rgba(99, 102, 241, 0.2)', padding: '2px 6px', borderRadius: '4px', fontFamily: 'monospace' }}>'caminho/arquivo.ext'</code>.
          </div>

          <div className="filelist">
            {loadingFiles ? (
              <div className="loader-container">
                <div className="loader"></div>
                <div className="muted" style={{ textAlign: 'center', marginTop: '16px', opacity: 0.8 }}>
                  ⏳ Carregando arquivos...
                </div>
              </div>
            ) : files.length === 0 ? (
              <div className="muted" style={{ textAlign: 'center', padding: '24px', opacity: 0.6 }}>
                📄 Nenhum arquivo importado ainda.
              </div>
            ) : (
              files.map((f) => (
                <div className="file" key={f.path}>
                  <div title={f.path} style={{ overflow: 'hidden', textOverflow: 'ellipsis' }}>
                    {f.path}
                  </div>
                  <div className="muted">{bytes(f.size)}</div>
                </div>
              ))
            )}
          </div>
        </div>

        <div className="card">
          <h2>💻 SQL</h2>
          <div className="sql-editor-wrapper">
            <CodeMirror
              value={sql}
              onChange={(value) => setSql(value)}
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
            <button onClick={runQuery} disabled={running || exporting}>
              {running ? '⏳ Executando…' : '▶️ Executar'}
            </button>
            <button className="secondary" onClick={exportCSV} disabled={running || exporting}>
              {exporting ? '⏳ Exportando…' : '⬇️ Exportar CSV'}
            </button>
            {resultInfo ? <span className="pill ok">{resultInfo}</span> : null}
          </div>

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
                      {table.columns.map((c) => (
                        <th key={c}>{c}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {table.rows.map((r, idx) => (
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
  );
}
