import { test, expect, beforeAll, afterAll, beforeEach } from "bun:test";
import { existsSync, mkdirSync, rmSync, statSync } from "fs";
import { join } from "path";

const TEST_DIR = process.env["MONGO_TOOLS_CONFIG_DIR"]!;

const { loadClusters, addCluster, removeCluster, renameCluster } =
  await import("./cluster-manager.js");

beforeAll(() => {
  if (existsSync(TEST_DIR)) rmSync(TEST_DIR, { recursive: true, force: true });
  mkdirSync(TEST_DIR, { recursive: true });
});

afterAll(() => {
  rmSync(TEST_DIR, { recursive: true, force: true });
});

beforeEach(() => {
  const clustersFile = join(TEST_DIR, "clusters.json");
  if (existsSync(clustersFile)) rmSync(clustersFile);
});

test("loadClusters returns [] when no file exists", () => {
  expect(loadClusters()).toEqual([]);
});

test("addCluster persists a cluster", () => {
  addCluster({ name: "prod", uri: "mongodb://localhost:27017" });
  expect(loadClusters()).toEqual([{ name: "prod", uri: "mongodb://localhost:27017" }]);
});

test("addCluster throws on duplicate name", () => {
  addCluster({ name: "prod", uri: "mongodb://localhost:27017" });
  expect(() => addCluster({ name: "prod", uri: "mongodb://other:27017" })).toThrow("already exists");
});

test("addCluster allows multiple clusters with different names", () => {
  addCluster({ name: "prod", uri: "mongodb://localhost:27017" });
  addCluster({ name: "staging", uri: "mongodb://localhost:27018" });
  expect(loadClusters()).toHaveLength(2);
});

test("removeCluster removes the named cluster", () => {
  addCluster({ name: "prod", uri: "mongodb://localhost:27017" });
  addCluster({ name: "staging", uri: "mongodb://localhost:27018" });
  removeCluster("prod");
  expect(loadClusters().map((c) => c.name)).toEqual(["staging"]);
});

test("removeCluster is a no-op for unknown name", () => {
  addCluster({ name: "prod", uri: "mongodb://localhost:27017" });
  removeCluster("nonexistent");
  expect(loadClusters()).toHaveLength(1);
});

test("renameCluster updates name and preserves uri", () => {
  addCluster({ name: "prod", uri: "mongodb://localhost:27017" });
  renameCluster("prod", "production");
  const clusters = loadClusters();
  expect(clusters[0].name).toBe("production");
  expect(clusters[0].uri).toBe("mongodb://localhost:27017");
});

test("clusters.json is written with mode 0o600", () => {
  addCluster({ name: "test", uri: "mongodb://localhost" });
  const stat = statSync(join(TEST_DIR, "clusters.json"));
  expect(stat.mode & 0o777).toBe(0o600);
});
