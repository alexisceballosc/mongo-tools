import { spinner, tasks, log } from "@clack/prompts";
import { selectDatabase, selectCloneTarget, confirmAction, selectTargetCluster, promptNewCluster } from "../ui/prompts.js";
import { showOperationPreview, showSummary } from "../ui/visualizers.js";
import { MongoClient } from "../utils/mongo-client.js";
import { addCluster, loadClusters } from "../utils/cluster-manager.js";
import type { Cluster, ImportResult } from "../config/types.js";

export async function cloneOperation(cluster: Cluster, client: MongoClient): Promise<void> {
  const fetchSpin = spinner();
  fetchSpin.start("Fetching databases...");
  const dbs = await client.listDatabases();
  fetchSpin.stop(`Found ${dbs.length} database(s).`);

  const sourceDb = await selectDatabase(dbs, "Select source database:");
  const target = await selectCloneTarget(dbs, sourceDb);

  if (target.type === "same") {
    const targetDb = target.dbName;

    const statsSpin = spinner();
    statsSpin.start("Fetching stats...");
    const sourceStats = await client.getDbStats(sourceDb);
    const targetStats = dbs.find((d) => d.name === targetDb)
      ? await client.getDbStats(targetDb)
      : null;
    statsSpin.stop("Stats loaded.");

    showOperationPreview("clone", sourceDb, targetDb, sourceStats, targetStats);

    const confirmed = await confirmAction(
      `Clone '${sourceDb}' into '${targetDb}'?${targetStats ? " Target will be overwritten." : ""}`
    );
    if (!confirmed) { log.warn("Clone cancelled."); return; }

    const start = Date.now();

    await tasks([
      {
        title: `Clearing '${targetDb}'`,
        enabled: !!targetStats,
        task: async () => {
          await client.dropDb(targetDb);
        },
      },
      {
        title: "Cloning collections",
        task: async (message) => {
          await client.cloneDb(sourceDb, targetDb, (name, i, total) => {
            message(`${i}/${total}: ${name}`);
          });
          return `${sourceStats.collections.length} collections, ${sourceStats.totalDocuments.toLocaleString()} docs`;
        },
      },
    ]);

    showSummary("Clone complete", sourceStats.collections.length, sourceStats.totalDocuments, Date.now() - start);
    log.success(`'${sourceDb}' cloned into '${targetDb}'.`);
    return;
  }

  let targetCluster;
  while (true) {
    const otherClusters = loadClusters().filter((c) => c.name !== cluster.name);

    if (otherClusters.length === 0) {
      log.error("No other clusters available. Add one from the main screen.");
      return;
    }

    const choice = await selectTargetCluster(otherClusters);

    if (choice === "new") {
      const newCluster = await promptNewCluster(loadClusters().map((c) => c.name));
      addCluster(newCluster);
      log.success(`Cluster '${newCluster.name}' saved.`);
      continue;
    }

    targetCluster = choice;
    break;
  }

  const connectSpin = spinner();
  connectSpin.start(`Connecting to '${targetCluster.name}'...`);
  const targetClient = new MongoClient();
  try {
    await targetClient.connect(targetCluster.uri);
    connectSpin.stop(`Connected to '${targetCluster.name}'.`);
  } catch (err: unknown) {
    connectSpin.stop("Connection failed.");
    log.error(err instanceof Error ? err.message : String(err));
    return;
  }

  try {
    const targetDbsFetch = spinner();
    targetDbsFetch.start("Fetching target databases...");
    const targetDbs = await targetClient.listDatabases();
    targetDbsFetch.stop(`Found ${targetDbs.length} database(s).`);

    const targetDb = await selectDatabase(targetDbs, `Target database on '${targetCluster.name}':`, true);

    const statsSpin = spinner();
    statsSpin.start("Fetching stats...");
    const sourceStats = await client.getDbStats(sourceDb);
    const targetStats = targetDbs.find((d) => d.name === targetDb)
      ? await targetClient.getDbStats(targetDb)
      : null;
    statsSpin.stop("Stats loaded.");

    showOperationPreview(
      "copy",
      `[${cluster.name}] ${sourceDb}`,
      `[${targetCluster.name}] ${targetDb}`,
      sourceStats,
      targetStats
    );

    const confirmed = await confirmAction(
      `Clone '${sourceDb}' from '${cluster.name}' into '${targetDb}' on '${targetCluster.name}'?`
    );
    if (!confirmed) {
      log.warn("Clone cancelled.");
      return;
    }

    const start = Date.now();
    let cloned: ImportResult = { collections: 0, documents: 0 };

    await tasks([
      {
        title: `Clearing '${targetDb}' on target`,
        enabled: !!targetStats,
        task: async () => {
          await targetClient.dropDb(targetDb);
        },
      },
      {
        title: "Cloning to target cluster",
        task: async (message) => {
          cloned = await client.cloneDbToClient(sourceDb, targetClient, targetDb, (name, i, total) => {
            message(`${i}/${total}: ${name}`);
          });
          return `${cloned.documents.toLocaleString()} docs cloned`;
        },
      },
    ]);

    showSummary("Clone complete", cloned.collections, cloned.documents, Date.now() - start);
    log.success(`'${sourceDb}' cloned into '${targetDb}' on '${targetCluster.name}'.`);
  } finally {
    await targetClient.disconnect();
  }
}
