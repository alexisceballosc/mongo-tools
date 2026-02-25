import { spinner, tasks, log } from "@clack/prompts";
import { selectDatabase, promptExistingPath, confirmAction } from "../ui/prompts.js";
import { showOperationPreview, showSummary } from "../ui/visualizers.js";
import { MongoClient } from "../utils/mongo-client.js";
import { existsSync } from "fs";
import type { Cluster, ImportResult } from "../config/types.js";

export async function uploadOperation(cluster: Cluster, client: MongoClient): Promise<void> {
  const localPath = await promptExistingPath("Path to local dump folder:");

  if (!existsSync(localPath)) {
    log.error(`Directory not found: ${localPath}`);
    return;
  }

  const fetchSpin = spinner();
  fetchSpin.start("Fetching databases...");
  const dbs = await client.listDatabases();
  fetchSpin.stop(`Found ${dbs.length} database(s).`);

  const targetDb = await selectDatabase(dbs, "Target database (will be replaced):", true);

  const statsSpin = spinner();
  statsSpin.start("Fetching target stats...");
  const targetStats = dbs.find((d) => d.name === targetDb)
    ? await client.getDbStats(targetDb)
    : null;
  statsSpin.stop("Done.");

  showOperationPreview("upload", localPath, targetDb, null, targetStats);

  const confirmed = await confirmAction(
    `Upload '${localPath}' into '${targetDb}'? Target data will be replaced.`
  );
  if (!confirmed) { log.warn("Upload cancelled."); return; }

  const start = Date.now();
  let result: ImportResult = { collections: 0, documents: 0 };

  await tasks([
    {
      title: `Clearing '${targetDb}'`,
      enabled: !!targetStats,
      task: async () => {
        await client.dropDb(targetDb);
      },
    },
    {
      title: "Importing collections",
      task: async (message) => {
        result = await client.importDb(targetDb, localPath, (name, i, total) => {
          message(`${i}/${total}: ${name}`);
        });
        return `${result.collections} collections, ${result.documents.toLocaleString()} docs`;
      },
    },
  ]);

  showSummary("Upload complete", result.collections, result.documents, Date.now() - start);
  log.success(`'${localPath}' uploaded into '${targetDb}'.`);
}
