import pc from "picocolors";
import type { DbStats } from "../config/types.js";

function truncate(str: string, width: number): string {
  if (str.length <= width) return str;
  return "\u2026" + str.slice(-(width - 1));
}

function pad(str: string, width: number): string {
  const s = truncate(str, width);
  return s.length >= width ? s : s + " ".repeat(width - s.length);
}

function formatNumber(n: number): string {
  return n.toLocaleString("en-US");
}

export function showOperationPreview(
  action: string,
  sourceLabel: string,
  targetLabel: string,
  sourceStats: DbStats | null,
  targetStats: DbStats | null
): void {
  const COL_WIDTH = 28;
  const isDestructive = ["OVERWRITE", "CLONE", "UPLOAD", "COPY"].includes(action.toUpperCase());

  const actionColor = isDestructive ? pc.red : pc.cyan;

  const sourceCollections = sourceStats ? `${sourceStats.collections.length} collection(s)` : "unknown";
  const sourceDocs = sourceStats ? `${formatNumber(sourceStats.totalDocuments)} docs` : "";
  const targetCollections = targetStats ? `${targetStats.collections.length} collection(s)` : "new / empty";
  const targetDocs = targetStats ? `${formatNumber(targetStats.totalDocuments)} docs` : "";

  const divider = "─".repeat(COL_WIDTH * 2 + 7);

  console.log("");
  console.log(pc.dim(divider));
  console.log(
    `  ${pc.bold(pad("SOURCE", COL_WIDTH))}   ${pc.bold("TARGET")}`
  );
  console.log(pc.dim(divider));
  console.log(
    `  ${pc.cyan(pad(sourceLabel, COL_WIDTH))}${pc.dim(" → ")}${actionColor(targetLabel)}`
  );
  console.log(
    `  ${pc.dim(pad(sourceCollections, COL_WIDTH))}   ${pc.dim(targetCollections)}`
  );
  if (sourceDocs || targetDocs) {
    console.log(
      `  ${pc.dim(pad(sourceDocs, COL_WIDTH))}   ${pc.dim(targetDocs)}`
    );
  }
  console.log(pc.dim(divider));
  console.log(
    `  Action: ${actionColor(pc.bold(action.toUpperCase()))}`
  );
  if (isDestructive && targetStats) {
    console.log(
      `  ${pc.red("Target data will be replaced.")}`
    );
  }
  console.log(pc.dim(divider));
  console.log("");
}

export function showSummary(label: string, collections: number, documents: number, ms: number): void {
  const secs = (ms / 1000).toFixed(1);
  console.log("");
  console.log(pc.dim("─".repeat(44)));
  console.log(`  ${pc.bold(label)}`);
  console.log(`  ${pc.dim("Collections:")} ${collections}`);
  console.log(`  ${pc.dim("Documents:  ")} ${formatNumber(documents)}`);
  console.log(`  ${pc.dim("Time:       ")} ${secs}s`);
  console.log(pc.dim("─".repeat(44)));
  console.log("");
}
