import { chmodSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { CONFIG_DIR, CLUSTERS_FILE } from "../config/constants.js";
import type { Cluster } from "../config/types.js";

function ensureConfigDir(): void {
  if (!existsSync(CONFIG_DIR)) {
    mkdirSync(CONFIG_DIR, { recursive: true, mode: 0o700 });
  }
}

export function loadClusters(): Cluster[] {
  ensureConfigDir();
  if (!existsSync(CLUSTERS_FILE)) return [];
  try {
    return JSON.parse(readFileSync(CLUSTERS_FILE, "utf-8")) as Cluster[];
  } catch {
    return [];
  }
}

export function saveClusters(clusters: Cluster[]): void {
  ensureConfigDir();
  writeFileSync(CLUSTERS_FILE, JSON.stringify(clusters, null, 2), { encoding: "utf-8", mode: 0o600 });
  chmodSync(CLUSTERS_FILE, 0o600);
}

export function addCluster(cluster: Cluster): void {
  const clusters = loadClusters();
  if (clusters.some((c) => c.name === cluster.name)) {
    throw new Error(`A cluster named '${cluster.name}' already exists.`);
  }
  clusters.push(cluster);
  saveClusters(clusters);
}

export function removeCluster(name: string): void {
  const clusters = loadClusters().filter((c) => c.name !== name);
  saveClusters(clusters);
}

export function renameCluster(oldName: string, newName: string): void {
  const clusters = loadClusters().map((c) =>
    c.name === oldName ? { ...c, name: newName } : c
  );
  saveClusters(clusters);
}
