export type ImportedFile = {
  path: string;
  size: number;
  type: string;
};

export function isSupportedFilePath(path: string) {
  const lower = path.toLowerCase();
  return (
    lower.endsWith('.parquet') ||
    lower.endsWith('.csv') ||
    lower.endsWith('.json') ||
    lower.endsWith('.ndjson')
  );
}

export async function collectFilesFromDirectoryHandle(
  rootHandle: any,
  basePath = '',
): Promise<{ files: File[]; meta: ImportedFile[] }> {
  const files: File[] = [];
  const meta: ImportedFile[] = [];

  for await (const [name, handle] of rootHandle.entries()) {
    const path = basePath ? `${basePath}/${name}` : name;
    if (handle.kind === 'directory') {
      const nested = await collectFilesFromDirectoryHandle(handle, path);
      files.push(...nested.files);
      meta.push(...nested.meta);
      continue;
    }
    if (handle.kind !== 'file') continue;
    if (!isSupportedFilePath(path)) continue;
    const file = await handle.getFile();
    files.push(file);
    meta.push({ path, size: file.size, type: file.type || 'file' });
  }

  return { files, meta };
}

export function collectFilesFromFileList(fileList: FileList) {
  const files: File[] = [];
  const meta: ImportedFile[] = [];
  for (const file of Array.from(fileList)) {
    const path = (file as any).webkitRelativePath || file.name;
    if (!isSupportedFilePath(path)) continue;
    files.push(file);
    meta.push({ path, size: file.size, type: file.type || 'file' });
  }
  return { files, meta };
}

