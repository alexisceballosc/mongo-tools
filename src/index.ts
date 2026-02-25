import { intro, outro, select, spinner, log, isCancel } from "@clack/prompts";
import pc from "picocolors";

import {
  loadClusters,
  addCluster,
  removeCluster,
  renameCluster,
} from "./utils/cluster-manager.js";
import { MongoClient } from "./utils/mongo-client.js";
import {
  promptNewCluster,
  selectCluster,
  selectClusterToManage,
  promptText,
  confirmAction,
  pressEnterToContinue,
  clearScreen,
} from "./ui/prompts.js";
import { cloneOperation } from "./operations/clone.js";
import { downloadOperation } from "./operations/download.js";
import { uploadOperation } from "./operations/upload.js";
import { dropOperation } from "./operations/drop.js";
import type { Cluster } from "./config/types.js";
import { GoBackError } from "./config/errors.js";

type MenuOption = "clone" | "download" | "upload" | "drop" | "back" | "exit";

async function manageClusters(): Promise<boolean> {
  try {
    const clusters = loadClusters();

    const action = await select({
      message: "Manage clusters:",
      options: [
        { value: "add", label: "Add cluster" },
        { value: "remove", label: "Remove cluster" },
        { value: "rename", label: "Rename cluster" },
        { value: "list", label: "List clusters" },
        { value: "back", label: "Back" },
      ],
    });

    if (isCancel(action) || action === "back") return false;

    if (action === "add") {
      const cluster = await promptNewCluster(clusters.map((c) => c.name));
      addCluster(cluster);
      log.success(`Cluster '${cluster.name}' added.`);
    } else if (action === "remove") {
      if (clusters.length === 0) { log.warn("No clusters saved."); return true; }
      const target = await selectClusterToManage(clusters, "Remove which cluster?");
      const ok = await confirmAction(`Remove '${target.name}'?`);
      if (ok) { removeCluster(target.name); log.success(`'${target.name}' removed.`); }
    } else if (action === "rename") {
      if (clusters.length === 0) { log.warn("No clusters saved."); return true; }
      const target = await selectClusterToManage(clusters, "Rename which cluster?");
      const newName = await promptText("New name:", target.name);
      renameCluster(target.name, newName);
      log.success(`Renamed to '${newName}'.`);
    } else if (action === "list") {
      const current = loadClusters();
      if (current.length === 0) {
        log.info("No clusters saved.");
      } else {
        console.log("");
        current.forEach((c, i) => {
          console.log(`  ${pc.dim(`${i + 1}.`)} ${pc.bold(c.name)}`);
          console.log(`     ${pc.dim(c.uri.slice(0, 60) + (c.uri.length > 60 ? "..." : ""))}`);
        });
        console.log("");
      }
    }

    return true;
  } catch (err) {
    if (err instanceof GoBackError) return false;
    throw err;
  }
}

async function connectToCluster(cluster: Cluster): Promise<MongoClient | null> {
  const spin = spinner();
  spin.start(`Connecting to '${cluster.name}'...`);
  const client = new MongoClient();
  try {
    await client.connect(cluster.uri);
    spin.stop(`Connected to '${cluster.name}'.`);
    return client;
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    spin.error(`Could not connect to '${cluster.name}': ${message}`);
    return null;
  }
}

async function runClusterSelection(): Promise<{ cluster: Cluster; client: MongoClient }> {
  while (true) {
    try {
      const clusters = loadClusters();

      if (clusters.length === 0) {
        log.info("No clusters configured. Add your first cluster.");
        const cluster = await promptNewCluster(clusters.map((c) => c.name));
        addCluster(cluster);
        log.success(`Cluster '${cluster.name}' saved.`);
        clearScreen();
        continue;
      }

      const choice = await selectCluster(clusters);

      if (choice === "manage") {
        const didAction = await manageClusters();
        if (didAction) await pressEnterToContinue();
        clearScreen();
        continue;
      }

      const client = await connectToCluster(choice);
      if (!client) {
        await pressEnterToContinue();
        clearScreen();
        continue;
      }

      clearScreen();
      return { cluster: choice, client };
    } catch (err) {
      if (err instanceof GoBackError) {
        process.exit(0);
      }
      throw err;
    }
  }
}

async function main(): Promise<void> {
  console.log("");
  intro(pc.inverse(pc.bold("  mongo-tools  ")));

  let { cluster: activeCluster, client } = await runClusterSelection();

  while (true) {
    const option = await select<MenuOption>({
      message: `${pc.dim(`[${activeCluster.name}]`)} What do you want to do?`,
      options: [
        { value: "clone",    label: "Clone database",    hint: "same or another cluster" },
        { value: "download", label: "Download database", hint: "export to local files" },
        { value: "upload",   label: "Upload database",   hint: "import from local files" },
        { value: "drop",     label: "Drop database",     hint: "permanently delete" },
        { value: "back",     label: "Back" },
        { value: "exit",     label: "Exit" },
      ],
    });

    if (isCancel(option) || option === "exit") {
      await client.disconnect();
      outro("Done.");
      process.exit(0);
    }

    if (option === "back") {
      await client.disconnect();
      clearScreen();
      const result = await runClusterSelection();
      activeCluster = result.cluster;
      client = result.client;
      continue;
    }

    try {
      if (option === "clone") {
        await cloneOperation(activeCluster, client);
      } else if (option === "download") {
        await downloadOperation(activeCluster, client);
      } else if (option === "upload") {
        await uploadOperation(activeCluster, client);
      } else if (option === "drop") {
        await dropOperation(activeCluster, client);
      }
      await pressEnterToContinue();
    } catch (err: unknown) {
      if (err instanceof GoBackError) {
        clearScreen();
        continue;
      }
      log.error(`Operation failed: ${err instanceof Error ? err.message : String(err)}`);
      await pressEnterToContinue();
    }
    clearScreen();
  }
}

main().catch((err) => {
  log.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
