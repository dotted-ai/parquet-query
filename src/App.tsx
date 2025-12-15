import { useMemo, useRef, useState, type ChangeEvent } from 'react';
import { tableToRows } from './arrow';
import {
  collectFilesFromDirectoryHandle,
  collectFilesFromFileList,
  type ImportedFile,
} from './fileImport';
import { getDuckDB, query, registerFileBuffer } from './duckdb';

const DEFAULT_SQL = `-- Dica: você pode consultar arquivos diretamente pelo caminho registrado.
-- Exemplos:
--   SELECT * FROM 'meus_dados/arquivo.parquet' LIMIT 50;
--   SELECT COUNT(*) FROM 'meus_dados/arquivo.csv';

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

export default function App() {
  const [dbStatus, setDbStatus] = useState<'idle' | 'loading' | 'ready'>('idle');
  const [files, setFiles] = useState<ImportedFile[]>([]);
  const [folderName, setFolderName] = useState<string>('');
  const [sql, setSql] = useState(DEFAULT_SQL);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string>('');
  const [resultInfo, setResultInfo] = useState<string>('');
  const [table, setTable] = useState<{ columns: string[]; rows: string[][] }>();

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

  async function importFiles(selectedFiles: File[], meta: ImportedFile[], label: string) {
    setError('');
    setResultInfo('');
    setTable(undefined);

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

  return (
    <div className="container">
      <div className="header">
        <h1 className="title">Parquet Query (React + DuckDB)</h1>
        <div className="muted">
          {dbStatus === 'ready' ? (
            <span className="pill ok">DuckDB pronto</span>
          ) : dbStatus === 'loading' ? (
            <span className="pill">Carregando DuckDB…</span>
          ) : (
            <span className="pill">DuckDB ainda não inicializado</span>
          )}
        </div>
      </div>

      <div className="grid">
        <div className="card">
          <h2>Arquivos</h2>
          <div className="row">
            <button onClick={onPickFolder} disabled={dbStatus === 'loading'}>
              {supportsDirectoryPicker ? 'Selecionar pasta' : 'Selecionar pasta (fallback)'}
            </button>
            <button
              className="secondary"
              onClick={() => {
                setFiles([]);
                setFolderName('');
                setResultInfo('');
                setTable(undefined);
                setError('');
              }}
              disabled={dbStatus === 'loading'}
            >
              Limpar
            </button>
            {folderName ? <span className="pill">{folderName}</span> : null}
            {files.length ? <span className="pill">{files.length} arquivos</span> : null}
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

          <div className="muted" style={{ marginTop: 10 }}>
            Suporta: parquet, csv, json, ndjson. Após importar, consulte com{' '}
            <code>'caminho/arquivo.ext'</code>.
          </div>

          <div className="filelist">
            {files.length === 0 ? (
              <div className="muted">Nenhum arquivo importado ainda.</div>
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
          <h2>SQL</h2>
          <textarea value={sql} onChange={(e) => setSql(e.target.value)} />
          <div className="row" style={{ marginTop: 10 }}>
            <button onClick={runQuery} disabled={running}>
              {running ? 'Executando…' : 'Executar'}
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
              <div className="muted" style={{ marginTop: 8 }}>
                Mostrando até 200 linhas.
              </div>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
