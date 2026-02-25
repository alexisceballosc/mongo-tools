import { test, expect, beforeEach, afterEach, spyOn } from "bun:test";
import { showSummary, showOperationPreview } from "./visualizers.js";
import type { DbStats } from "../config/types.js";

let logSpy: ReturnType<typeof spyOn<Console, "log">>;

beforeEach(() => {
  logSpy = spyOn(console, "log").mockImplementation(() => {});
});

afterEach(() => {
  logSpy.mockRestore();
});

function output(): string {
  return logSpy.mock.calls.map(([msg]) => String(msg ?? "")).join("\n");
}

test("showSummary includes the label", () => {
  showSummary("Clone complete", 3, 1500, 2500);
  expect(output()).toContain("Clone complete");
});

test("showSummary shows collection count", () => {
  showSummary("Done", 7, 0, 100);
  expect(output()).toContain("7");
});

test("showSummary formats document count with thousands separator", () => {
  showSummary("Done", 1, 1500, 100);
  expect(output()).toContain("1,500");
});

test("showSummary shows elapsed time in seconds", () => {
  showSummary("Done", 0, 0, 2500);
  expect(output()).toContain("2.5s");
});

test("showSummary shows sub-second time correctly", () => {
  showSummary("Done", 0, 0, 300);
  expect(output()).toContain("0.3s");
});

test("showOperationPreview includes source and target labels", () => {
  showOperationPreview("clone", "prod-db", "staging-db", null, null);
  const out = output();
  expect(out).toContain("prod-db");
  expect(out).toContain("staging-db");
});

test("showOperationPreview uppercases the action", () => {
  showOperationPreview("clone", "src", "tgt", null, null);
  expect(output()).toContain("CLONE");
});

test("showOperationPreview shows 'new / empty' when targetStats is null", () => {
  showOperationPreview("clone", "src", "tgt", null, null);
  expect(output()).toContain("new / empty");
});

test("showOperationPreview shows 'unknown' when sourceStats is null", () => {
  showOperationPreview("clone", "src", "tgt", null, null);
  expect(output()).toContain("unknown");
});

test("showOperationPreview shows collection count from sourceStats", () => {
  const stats: DbStats = {
    name: "src",
    collections: [{ name: "users", count: 10 }, { name: "posts", count: 5 }],
    totalDocuments: 15,
  };
  showOperationPreview("clone", "src", "tgt", stats, null);
  expect(output()).toContain("2 collection(s)");
});

test("showOperationPreview shows doc counts when both stats provided", () => {
  const srcStats: DbStats = {
    name: "src",
    collections: [{ name: "users", count: 1000 }],
    totalDocuments: 1000,
  };
  const tgtStats: DbStats = {
    name: "tgt",
    collections: [{ name: "users", count: 500 }],
    totalDocuments: 500,
  };
  showOperationPreview("clone", "src", "tgt", srcStats, tgtStats);
  const out = output();
  expect(out).toContain("1,000");
  expect(out).toContain("500");
});

test("showOperationPreview shows replacement warning for destructive action with target data", () => {
  const tgtStats: DbStats = { name: "tgt", collections: [], totalDocuments: 0 };
  showOperationPreview("clone", "src", "tgt", null, tgtStats);
  expect(output()).toContain("Target data will be replaced");
});

test("showOperationPreview does not show replacement warning when target is empty", () => {
  showOperationPreview("clone", "src", "tgt", null, null);
  expect(output()).not.toContain("Target data will be replaced");
});
