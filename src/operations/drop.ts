import { spinner, log, note } from "@clack/prompts";
import pc from "picocolors";
import { selectDatabase, confirmAction, promptNewPath } from "../ui/prompts.js";
import { MongoClient } from "../utils/mongo-client.js";
import type { Cluster } from "../config/types.js";

export async function dropOperation(cluster: Cluster, client: MongoClient): Promise<void> {
  const fetchSpin = spinner();
  fetchSpin.start("Fetching databases...");
  const dbs = await client.listDatabases();
  fetchSpin.stop(`Found ${dbs.length} database(s).`);

  const dbName = await selectDatabase(dbs, "Select database to drop:");

  const wantsBackup = await confirmAction(
    `Download a backup of '${dbName}' before dropping?`,
    true
  );

  if (wantsBackup) {
    const date = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
    const outDir = await promptNewPath(`${dbName}-before-drop-${date}`);

    const backupSpin = spinner();
    backupSpin.start(`Backing up '${dbName}'...`);
    const result = await client.exportDb(dbName, outDir, (name, i, total) => {
      backupSpin.message(`Exporting ${i}/${total}: ${name}`);
    });
    backupSpin.stop(`Backup saved to: ${result.path}`);
    log.success(`${result.collections} collection(s), ${result.documents.toLocaleString()} document(s) saved.`);
  } else {
    log.warn("Proceeding without backup. This cannot be undone.");
  }

  note(pc.red(pc.bold(dbName)), "About to permanently delete");

  const confirmed = await confirmAction(`Drop '${dbName}' permanently?`);
  if (!confirmed) {
    log.warn("Drop cancelled.");
    return;
  }

  const dropSpin = spinner();
  dropSpin.start(`Dropping '${dbName}'...`);
  await client.dropDb(dbName);
  dropSpin.stop(`'${dbName}' dropped.`);

  log.success(`'${dbName}' has been permanently deleted.`);
}
