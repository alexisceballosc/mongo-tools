import {
  select,
  autocomplete,
  text,
  confirm,
  isCancel,
  cancel,
  log,
  password,
  path,
} from "@clack/prompts";
import pc from "picocolors";
import { join } from "path";
import type { Cluster, DbInfo } from "../config/types.js";
import { GoBackError } from "../config/errors.js";

function handleCancel(value: unknown): never | void {
  if (isCancel(value)) {
    cancel("Cancelled.");
    throw new GoBackError();
  }
}

export async function promptNewCluster(existingNames: string[] = []): Promise<Cluster> {
  const name = await text({
    message: "Cluster name (friendly label):",
    placeholder: "e.g. Niko Production",
    validate: (v) => {
      if (!v?.trim()) return "Name is required.";
      if (existingNames.includes(v.trim())) return `'${v.trim()}' already exists.`;
      return undefined;
    },
  });
  handleCancel(name);

  const uri = await password({
    message: "MongoDB URI:",
    validate: (v) =>
      v?.trim().startsWith("mongodb") ? undefined : "Must start with mongodb:// or mongodb+srv://",
  });
  handleCancel(uri);

  log.warn("This URI will be stored in plaintext at ~/.config/mongo-tools/clusters.json — keep this file private.");

  return { name: name as string, uri: uri as string };
}

export async function selectCluster(clusters: Cluster[]): Promise<Cluster | "manage"> {
  const options = [
    ...clusters.map((c) => ({ value: c.name, label: c.name })),
    { value: "__manage__", label: "Manage clusters" },
  ];

  const choice = await autocomplete({
    message: "Select cluster:",
    options,
  });
  handleCancel(choice);

  if (choice === "__manage__") return "manage";
  return clusters.find((c) => c.name === choice)!;
}

export async function selectDatabase(
  dbs: DbInfo[],
  message: string,
  allowNew = false
): Promise<string> {
  const options = [
    ...dbs.map((d) => ({
      value: d.name,
      label: d.name,
      hint: d.sizeOnDisk ? `${(d.sizeOnDisk / 1024 / 1024).toFixed(1)} MB` : undefined,
    })),
    ...(allowNew ? [{ value: "__new__", label: "+ Create new database" }] : []),
  ];

  const choice = await autocomplete({ message, options });
  handleCancel(choice);

  if (choice === "__new__") {
    const name = await text({
      message: "New database name:",
      validate: (v) => (!v?.trim() ? "Name is required." : undefined),
    });
    handleCancel(name);
    return name as string;
  }

  return choice as string;
}

export async function selectCloneTarget(
  dbs: DbInfo[],
  sourceDb: string
): Promise<{ type: "same"; dbName: string } | { type: "other" }> {
  const options = [
    ...dbs
      .filter((d) => d.name !== sourceDb)
      .map((d) => ({
        value: d.name,
        label: d.name,
        hint: d.sizeOnDisk ? `${(d.sizeOnDisk / 1024 / 1024).toFixed(1)} MB` : undefined,
      })),
    { value: "__new__", label: "+ Create new database" },
    { value: "__other__", label: "→ Another cluster" },
  ];

  const choice = await autocomplete({ message: "Select target:", options });
  handleCancel(choice);

  if (choice === "__other__") return { type: "other" };

  if (choice === "__new__") {
    const name = await text({
      message: "New database name:",
      validate: (v) => (!v?.trim() ? "Name is required." : undefined),
    });
    handleCancel(name);
    return { type: "same", dbName: name as string };
  }

  return { type: "same", dbName: choice as string };
}

export async function promptNewPath(defaultName: string): Promise<string> {
  const dir = await path({
    message: "Output directory:",
    root: process.cwd(),
    directory: true,
  });
  handleCancel(dir);

  const name = await text({
    message: "Folder name:",
    initialValue: defaultName,
    validate: (v) => (!v?.trim() ? "Name is required." : undefined),
  });
  handleCancel(name);

  return join(dir as string, name as string);
}

export async function promptExistingPath(message: string): Promise<string> {
  const dir = await path({
    message,
    root: process.cwd(),
    directory: true,
  });
  handleCancel(dir);
  return dir as string;
}

export async function confirmAction(message: string, defaultValue = false): Promise<boolean> {
  const ok = await confirm({ message, initialValue: defaultValue });
  handleCancel(ok);
  return ok as boolean;
}

export async function selectClusterToManage(
  clusters: Cluster[],
  message: string
): Promise<Cluster> {
  const choice = await autocomplete({
    message,
    options: clusters.map((c) => ({ value: c.name, label: c.name })),
  });
  handleCancel(choice);
  return clusters.find((c) => c.name === choice)!;
}

export async function promptText(message: string, placeholder?: string): Promise<string> {
  const val = await text({ message, placeholder });
  handleCancel(val);
  return val as string;
}

export async function selectTargetCluster(clusters: Cluster[]): Promise<Cluster | "new"> {
  const options = [
    ...clusters.map((c) => ({ value: c.name, label: c.name })),
    { value: "__new__", label: "+ Add new cluster" },
  ];

  const choice = await select({ message: "Target cluster:", options });
  handleCancel(choice);

  if (choice === "__new__") return "new";
  return clusters.find((c) => c.name === choice)!;
}

export async function pressEnterToContinue(): Promise<void> {
  process.stdout.write(pc.dim("\n  Press Enter to continue..."));
  await new Promise<void>((resolve) => {
    process.stdin.resume();
    process.stdin.once("data", () => {
      process.stdin.pause();
      resolve();
    });
  });
}

export function clearScreen(): void {
  console.clear();
}
