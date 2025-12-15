import { useMemo, useRef, useState, type ChangeEvent } from 'react';
import { tableToRows } from './arrow';
import {
  collectFilesFromDirectoryHandle,
  collectFilesFromFileList,
  type ImportedFile,
} from './fileImport';
import { exec, getDuckDB, query, registerFileBuffer } from './duckdb';

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
  const [error, setError] = useState<string>('');
  const [resultInfo, setResultInfo] = useState<string>('');
  const [importInfo, setImportInfo] = useState<string>('');
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
            <button onClick={onPickFolder} disabled={dbStatus === 'loading'}>
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
              disabled={dbStatus === 'loading'}
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
            {files.length === 0 ? (
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
          <textarea value={sql} onChange={(e) => setSql(e.target.value)} />
          <div className="row" style={{ marginTop: 10 }}>
            <button onClick={runQuery} disabled={running}>
              {running ? '⏳ Executando…' : '▶️ Executar'}
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
