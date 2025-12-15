import type { Table } from 'apache-arrow';

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'bigint') return value.toString();
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

function csvEscape(value: string): string {
  if (value.includes('"') || value.includes(',') || value.includes('\n') || value.includes('\r')) {
    return `"${value.replaceAll('"', '""')}"`;
  }
  return value;
}

function csvLine(cells: string[]) {
  return `${cells.map(csvEscape).join(',')}\r\n`;
}

export function tableToRows(table: Table, limit: number): {
  columns: string[];
  rows: string[][];
} {
  const columns = table.schema.fields.map((f) => f.name);
  const cols = columns.map((_, idx) => table.getChildAt(idx));
  const rowCount = Math.min(table.numRows, limit);

  const rows: string[][] = [];
  for (let rowIdx = 0; rowIdx < rowCount; rowIdx++) {
    const row: string[] = [];
    for (let colIdx = 0; colIdx < columns.length; colIdx++) {
      row.push(formatCell(cols[colIdx]?.get(rowIdx)));
    }
    rows.push(row);
  }
  return { columns, rows };
}

export async function recordBatchesToCSVParts(
  batches: AsyncIterable<any>,
  opts?: { header?: boolean; flushChars?: number },
): Promise<{ parts: string[]; rows: number; columns: number }> {
  const parts: string[] = [];
  const includeHeader = opts?.header ?? true;
  const flushChars = opts?.flushChars ?? 1_000_000;

  let buffer = '';
  let headerWritten = false;
  let columns = 0;
  let rows = 0;

  for await (const batch of batches) {
    if (!headerWritten) {
      columns = batch.schema.fields.length;
      if (includeHeader) buffer += csvLine(batch.schema.fields.map((f: any) => f.name));
      headerWritten = true;
    }

    for (let rowIdx = 0; rowIdx < batch.numRows; rowIdx++) {
      const row: string[] = [];
      for (let colIdx = 0; colIdx < columns; colIdx++) {
        row.push(formatCell(batch.getChildAt(colIdx)?.get(rowIdx)));
      }
      buffer += csvLine(row);
      rows++;

      if (buffer.length >= flushChars) {
        parts.push(buffer);
        buffer = '';
      }
    }
  }

  if (!headerWritten) {
    throw new Error('Query não retornou schema para exportação.');
  }
  if (buffer) parts.push(buffer);

  return { parts, rows, columns };
}
