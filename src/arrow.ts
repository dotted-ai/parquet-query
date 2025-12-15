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

