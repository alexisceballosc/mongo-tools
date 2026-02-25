import { spinner, log } from "@clack/prompts";
import { selectDatabase, promptNewPath } from "../ui/prompts.js";
import { showSummary } from "../ui/visualizers.js";
import { MongoClient } from "../utils/mongo-client.js";
import type { Cluster } from "../config/types.js";

export async function downloadOperation(cluster: Cluster, client: MongoClient): Promise<void> {
  const fetchSpin = spinner();
  fetchSpin.start("Fetching databases...");
  const dbs = await client.listDatabases();
  fetchSpin.stop(`Found ${dbs.length} database(s).`);

  const dbName = await selectDatabase(dbs, "Select database to download:");

  const date = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const outDir = await promptNewPath(`${dbName}-${date}`);

  const start = Date.now();
  const exportSpin = spinner();
  exportSpin.start(`Downloading '${dbName}'...`);

  const result = await client.exportDb(dbName, outDir, (name, i, total) => {
    exportSpin.message(`Exporting ${i}/${total}: ${name}`);
  });

  exportSpin.stop("Download complete.");

  showSummary("Download complete", result.collections, result.documents, Date.now() - start);
  log.success(`Saved to: ${result.path}`);
}
