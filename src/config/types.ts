export interface Cluster {
  name: string;
  uri: string;
}

export interface DbInfo {
  name: string;
  sizeOnDisk?: number;
}

export interface CollectionInfo {
  name: string;
  count: number;
}

export interface DbStats {
  name: string;
  collections: CollectionInfo[];
  totalDocuments: number;
}

export interface ExportResult {
  collections: number;
  documents: number;
  path: string;
}

export interface ImportResult {
  collections: number;
  documents: number;
}

