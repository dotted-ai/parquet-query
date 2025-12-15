import * as duckdb from '@duckdb/duckdb-wasm';
import type { Table } from 'apache-arrow';

import duckdbWasmMvp from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import duckdbWasmEh from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url';
import duckdbWasmCoi from '@duckdb/duckdb-wasm/dist/duckdb-coi.wasm?url';
import duckdbWorkerMvp from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url';
import duckdbWorkerEh from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url';
import duckdbWorkerCoi from '@duckdb/duckdb-wasm/dist/duckdb-browser-coi.worker.js?url';
import duckdbPthreadWorkerCoi from '@duckdb/duckdb-wasm/dist/duckdb-browser-coi.pthread.worker.js?url';

type DuckDBState = {
  db: duckdb.AsyncDuckDB;
  conn: duckdb.AsyncDuckDBConnection;
};

let statePromise: Promise<DuckDBState> | null = null;

export async function getDuckDB(): Promise<DuckDBState> {
  if (statePromise) return statePromise;
  statePromise = (async () => {
    const bundles: duckdb.DuckDBBundles = {
      mvp: {
        mainModule: duckdbWasmMvp,
        mainWorker: duckdbWorkerMvp,
      },
      eh: {
        mainModule: duckdbWasmEh,
        mainWorker: duckdbWorkerEh,
      },
      coi: {
        mainModule: duckdbWasmCoi,
        mainWorker: duckdbWorkerCoi,
        pthreadWorker: duckdbPthreadWorkerCoi,
      },
    };
    const bundle = await duckdb.selectBundle(bundles);
    if (!bundle.mainWorker) throw new Error('DuckDB bundle is missing a main worker');
    const worker = new Worker(bundle.mainWorker, { type: 'module' });
    const logger = new duckdb.ConsoleLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker ?? undefined);
    const conn = await db.connect();
    return { db, conn };
  })();
  return statePromise;
}

export async function registerFileBuffer(path: string, data: ArrayBuffer) {
  const { db } = await getDuckDB();
  await db.registerFileBuffer(path, new Uint8Array(data));
}

export async function query(sql: string): Promise<Table> {
  const { conn } = await getDuckDB();
  return (await conn.query(sql)) as unknown as Table;
}
