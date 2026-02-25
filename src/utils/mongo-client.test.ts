import { mock, test, expect, beforeEach, afterEach } from "bun:test";
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import { EJSON } from "bson";

const state = {
  dbs: {} as Record<string, Record<string, Record<string, unknown>[]>>,
  colIndexes: {} as Record<string, Record<string, unknown>[]>,
  insertBatches: {} as Record<string, Record<string, unknown>[][]>,
  createdIndexes: {} as Record<string, unknown[][]>,
};

function resetMockState() {
  state.dbs = {};
  state.colIndexes = {};
  state.insertBatches = {};
  state.createdIndexes = {};
}

mock.module("mongodb", () => {
  class MockClient {
    db(dbName: string) {
      state.dbs[dbName] ??= {};
      return {
        collection(colName: string) {
          const key = `${dbName}.${colName}`;
          return {
            find() {
              const docs = [...((state.dbs[dbName] ?? {})[colName] ?? [])];
              let i = 0;
              return {
                [Symbol.asyncIterator]() {
                  return {
                    async next(): Promise<any> {
                      if (i < docs.length) return { value: docs[i++], done: false };
                      return { value: undefined, done: true };
                    },
                  };
                },
              };
            },
            async insertMany(batch: Record<string, unknown>[]) {
              state.dbs[dbName] ??= {};
              state.dbs[dbName][colName] = [
                ...(state.dbs[dbName][colName] ?? []),
                ...batch,
              ];
              (state.insertBatches[key] ??= []).push([...batch]);
            },
            async countDocuments() {
              return ((state.dbs[dbName] ?? {})[colName] ?? []).length;
            },
            async indexes() {
              return (
                state.colIndexes[key] ?? [{ v: 2, name: "_id_", key: { _id: 1 } }]
              );
            },
            async createIndexes(specs: unknown[]) {
              (state.createdIndexes[key] ??= []).push([...specs]);
            },
          };
        },
        listCollections() {
          return {
            async toArray() {
              return Object.keys(state.dbs[dbName] ?? {}).map((name) => ({ name }));
            },
          };
        },
        async dropDatabase() {
          state.dbs[dbName] = {};
        },
        admin() {
          return {
            async listDatabases() {
              return {
                databases: Object.keys(state.dbs).map((name) => ({
                  name,
                  sizeOnDisk: 0,
                })),
              };
            },
          };
        },
      };
    }
    async connect() {}
    async close() {}
  }
  return { MongoClient: MockClient };
});

const { MongoClient } = await import("./mongo-client.js");

const TEST_DIR = join(tmpdir(), `mongo-client-test-${Date.now()}-${process.pid}`);

async function connected() {
  const c = new MongoClient();
  await c.connect("mongodb://mock");
  return c;
}

beforeEach(() => {
  resetMockState();
  mkdirSync(TEST_DIR, { recursive: true });
  for (const f of readdirSync(TEST_DIR)) rmSync(join(TEST_DIR, f), { recursive: true });
});

afterEach(() => {
  if (existsSync(TEST_DIR)) rmSync(TEST_DIR, { recursive: true, force: true });
});

test("throws before connect", async () => {
  const c = new MongoClient();
  await expect(c.listDatabases()).rejects.toThrow("Not connected");
});

test("throws after disconnect", async () => {
  const c = await connected();
  await c.disconnect();
  await expect(c.listDatabases()).rejects.toThrow("Not connected");
});

test("listCollections returns empty array for empty db", async () => {
  const c = await connected();
  state.dbs["db1"] = {};
  expect(await c.listCollections("db1")).toEqual([]);
});

test("listCollections returns name and count for each collection", async () => {
  const c = await connected();
  state.dbs["db1"] = {
    users: [{ name: "alice" }, { name: "bob" }],
    posts: [{ title: "hello" }],
  };
  const result = await c.listCollections("db1");
  expect(result).toHaveLength(2);
  expect(result.find((r) => r.name === "users")?.count).toBe(2);
  expect(result.find((r) => r.name === "posts")?.count).toBe(1);
});

test("exportDb creates one .jsonl file per collection", async () => {
  const c = await connected();
  state.dbs["db1"] = { users: [{ x: 1 }], posts: [{ y: 2 }] };

  await c.exportDb("db1", TEST_DIR);

  expect(existsSync(join(TEST_DIR, "users.jsonl"))).toBe(true);
  expect(existsSync(join(TEST_DIR, "posts.jsonl"))).toBe(true);
});

test("exportDb writes one EJSON line per document", async () => {
  const c = await connected();
  state.dbs["db1"] = { users: [{ name: "alice", age: 30 }, { name: "bob", age: 25 }] };

  await c.exportDb("db1", TEST_DIR);

  const lines = readFileSync(join(TEST_DIR, "users.jsonl"), "utf-8")
    .split("\n")
    .filter(Boolean);
  expect(lines).toHaveLength(2);
  expect(EJSON.parse(lines[0])).toMatchObject({ name: "alice", age: 30 });
  expect(EJSON.parse(lines[1])).toMatchObject({ name: "bob", age: 25 });
});

test("exportDb writes empty .jsonl for an empty collection", async () => {
  const c = await connected();
  state.dbs["db1"] = { empty: [] };

  await c.exportDb("db1", TEST_DIR);

  expect(readFileSync(join(TEST_DIR, "empty.jsonl"), "utf-8").trim()).toBe("");
});

test("exportDb writes .indexes.json excluding _id_", async () => {
  const c = await connected();
  state.dbs["db1"] = { users: [{ name: "alice" }] };
  state.colIndexes["db1.users"] = [
    { v: 2, name: "_id_", key: { _id: 1 } },
    { v: 2, name: "email_1", key: { email: 1 }, unique: true },
  ];

  await c.exportDb("db1", TEST_DIR);

  const indexes = JSON.parse(readFileSync(join(TEST_DIR, "users.indexes.json"), "utf-8"));
  expect(indexes).toHaveLength(1);
  expect(indexes[0].name).toBe("email_1");
  expect(indexes[0].unique).toBe(true);
});

test("exportDb writes empty .indexes.json when only _id_ exists", async () => {
  const c = await connected();
  state.dbs["db1"] = { users: [{ name: "alice" }] };

  await c.exportDb("db1", TEST_DIR);

  const indexes = JSON.parse(readFileSync(join(TEST_DIR, "users.indexes.json"), "utf-8"));
  expect(indexes).toHaveLength(0);
});

test("exportDb returns correct collection and document counts", async () => {
  const c = await connected();
  state.dbs["db1"] = {
    a: [{ x: 1 }, { x: 2 }],
    b: [{ x: 3 }],
  };

  const result = await c.exportDb("db1", TEST_DIR);

  expect(result.collections).toBe(2);
  expect(result.documents).toBe(3);
  expect(result.path).toBe(TEST_DIR);
});

test("importDb inserts docs from .jsonl files", async () => {
  const c = await connected();
  writeFileSync(join(TEST_DIR, "users.jsonl"), '{"name":"alice"}\n{"name":"bob"}\n', "utf-8");

  const result = await c.importDb("db1", TEST_DIR);

  expect(result.collections).toBe(1);
  expect(result.documents).toBe(2);
  expect(state.dbs["db1"]["users"]).toHaveLength(2);
  expect(state.dbs["db1"]["users"][0]).toMatchObject({ name: "alice" });
});

test("importDb batches inserts at BATCH_SIZE=500", async () => {
  const c = await connected();
  const lines = Array.from({ length: 1001 }, (_, i) => JSON.stringify({ i })).join("\n");
  writeFileSync(join(TEST_DIR, "col.jsonl"), lines + "\n", "utf-8");

  await c.importDb("db1", TEST_DIR);

  const batches = state.insertBatches["db1.col"];
  expect(batches).toHaveLength(3);
  expect(batches[0]).toHaveLength(500);
  expect(batches[1]).toHaveLength(500);
  expect(batches[2]).toHaveLength(1);
});

test("importDb restores indexes from .indexes.json stripping v and ns", async () => {
  const c = await connected();
  writeFileSync(join(TEST_DIR, "users.jsonl"), '{"name":"alice"}\n', "utf-8");
  writeFileSync(
    join(TEST_DIR, "users.indexes.json"),
    JSON.stringify([{ v: 2, ns: "db1.users", name: "email_1", key: { email: 1 }, unique: true }]),
    "utf-8"
  );

  await c.importDb("db1", TEST_DIR);

  const created = state.createdIndexes["db1.users"];
  expect(created).toHaveLength(1);
  const spec = created[0][0] as Record<string, unknown>;
  expect(spec["name"]).toBe("email_1");
  expect(spec["unique"]).toBe(true);
  expect(spec["v"]).toBeUndefined();
  expect(spec["ns"]).toBeUndefined();
});

test("importDb skips createIndexes when .indexes.json is absent", async () => {
  const c = await connected();
  writeFileSync(join(TEST_DIR, "users.jsonl"), '{"name":"alice"}\n', "utf-8");

  await c.importDb("db1", TEST_DIR);

  expect(state.createdIndexes["db1.users"]).toBeUndefined();
});

test("importDb does not count .indexes.json as a collection", async () => {
  const c = await connected();
  writeFileSync(join(TEST_DIR, "users.jsonl"), '{"name":"alice"}\n', "utf-8");
  writeFileSync(join(TEST_DIR, "users.indexes.json"), "[]", "utf-8");

  const result = await c.importDb("db1", TEST_DIR);

  expect(result.collections).toBe(1);
});

test("importDb skips _id_ index from .indexes.json", async () => {
  const c = await connected();
  writeFileSync(join(TEST_DIR, "users.jsonl"), '{"name":"alice"}\n', "utf-8");
  writeFileSync(
    join(TEST_DIR, "users.indexes.json"),
    JSON.stringify([{ name: "_id_", key: { _id: 1 } }]),
    "utf-8"
  );

  await c.importDb("db1", TEST_DIR);

  expect(state.createdIndexes["db1.users"]).toBeUndefined();
});

test("importDb throws when directory does not exist", async () => {
  const c = await connected();
  await expect(c.importDb("db1", "/nonexistent/xyz")).rejects.toThrow("Directory not found");
});

test("importDb returns zero counts for empty directory", async () => {
  const c = await connected();
  const result = await c.importDb("db1", TEST_DIR);
  expect(result.collections).toBe(0);
  expect(result.documents).toBe(0);
});

test("cloneDb copies all documents to the target db", async () => {
  const c = await connected();
  state.dbs["src"] = { users: [{ name: "alice" }, { name: "bob" }] };

  await c.cloneDb("src", "tgt");

  expect(state.dbs["tgt"]["users"]).toHaveLength(2);
  expect(state.dbs["tgt"]["users"][0]).toMatchObject({ name: "alice" });
});

test("cloneDb batches inserts at BATCH_SIZE=500", async () => {
  const c = await connected();
  state.dbs["src"] = {
    big: Array.from({ length: 1001 }, (_, i) => ({ i })),
  };

  await c.cloneDb("src", "tgt");

  const batches = state.insertBatches["tgt.big"];
  expect(batches).toHaveLength(3);
  expect(batches[0]).toHaveLength(500);
  expect(batches[1]).toHaveLength(500);
  expect(batches[2]).toHaveLength(1);
});

test("cloneDb copies user indexes excluding _id_, stripping v and ns", async () => {
  const c = await connected();
  state.dbs["src"] = { users: [{ name: "alice" }] };
  state.colIndexes["src.users"] = [
    { v: 2, name: "_id_", key: { _id: 1 } },
    { v: 2, ns: "src.users", name: "email_1", key: { email: 1 }, unique: true },
  ];

  await c.cloneDb("src", "tgt");

  const created = state.createdIndexes["tgt.users"];
  expect(created).toHaveLength(1);
  const spec = created[0][0] as Record<string, unknown>;
  expect(spec["name"]).toBe("email_1");
  expect(spec["unique"]).toBe(true);
  expect(spec["v"]).toBeUndefined();
  expect(spec["ns"]).toBeUndefined();
});

test("cloneDb skips createIndexes when only _id_ index exists", async () => {
  const c = await connected();
  state.dbs["src"] = { users: [{ name: "alice" }] };

  await c.cloneDb("src", "tgt");

  expect(state.createdIndexes["tgt.users"]).toBeUndefined();
});

test("cloneDb calls onCollection with name, index, and total", async () => {
  const c = await connected();
  state.dbs["src"] = { a: [{ x: 1 }], b: [{ x: 2 }] };

  const calls: [string, number, number][] = [];
  await c.cloneDb("src", "tgt", (name, i, total) => calls.push([name, i, total]));

  expect(calls).toHaveLength(2);
  expect(calls.every(([, , total]) => total === 2)).toBe(true);
  const names = calls.map(([name]) => name);
  expect(names).toContain("a");
  expect(names).toContain("b");
});

test("cloneDbToClient streams docs directly between two clients", async () => {
  const src = await connected();
  const tgt = await connected();
  state.dbs["source"] = { users: [{ name: "alice" }, { name: "bob" }] };

  const result = await src.cloneDbToClient("source", tgt, "target");

  expect(result.collections).toBe(1);
  expect(result.documents).toBe(2);
  expect(state.dbs["target"]["users"]).toHaveLength(2);
});

test("cloneDbToClient batches at BATCH_SIZE=500", async () => {
  const src = await connected();
  const tgt = await connected();
  state.dbs["source"] = {
    big: Array.from({ length: 1001 }, (_, i) => ({ i })),
  };

  await src.cloneDbToClient("source", tgt, "target");

  const batches = state.insertBatches["target.big"];
  expect(batches).toHaveLength(3);
  expect(batches[0]).toHaveLength(500);
  expect(batches[1]).toHaveLength(500);
  expect(batches[2]).toHaveLength(1);
});

test("cloneDbToClient copies user indexes", async () => {
  const src = await connected();
  const tgt = await connected();
  state.dbs["source"] = { users: [{ name: "alice" }] };
  state.colIndexes["source.users"] = [
    { v: 2, name: "_id_", key: { _id: 1 } },
    { v: 2, name: "name_1", key: { name: 1 } },
  ];

  await src.cloneDbToClient("source", tgt, "target");

  const created = state.createdIndexes["target.users"];
  expect(created).toHaveLength(1);
  expect((created[0][0] as Record<string, unknown>)["name"]).toBe("name_1");
});

test("cloneDbToClient returns correct collection and document counts", async () => {
  const src = await connected();
  const tgt = await connected();
  state.dbs["source"] = {
    a: [{ x: 1 }, { x: 2 }],
    b: [{ x: 3 }],
  };

  const result = await src.cloneDbToClient("source", tgt, "target");

  expect(result.collections).toBe(2);
  expect(result.documents).toBe(3);
});

test("dropDb removes all collections", async () => {
  const c = await connected();
  state.dbs["db1"] = { users: [{ name: "alice" }] };

  await c.dropDb("db1");

  expect(await c.listCollections("db1")).toEqual([]);
});

test("exportDb → importDb roundtrip preserves documents and indexes", async () => {
  const exporter = await connected();
  const importer = await connected();

  state.dbs["original"] = {
    users: [{ name: "alice", age: 30 }, { name: "bob", age: 25 }],
  };
  state.colIndexes["original.users"] = [
    { v: 2, name: "_id_", key: { _id: 1 } },
    { v: 2, name: "name_1", key: { name: 1 } },
  ];

  await exporter.exportDb("original", TEST_DIR);
  await importer.importDb("restored", TEST_DIR);

  expect(state.dbs["restored"]["users"]).toHaveLength(2);
  expect(state.dbs["restored"]["users"][0]).toMatchObject({ name: "alice" });

  const created = state.createdIndexes["restored.users"];
  expect(created).toHaveLength(1);
  expect((created[0][0] as Record<string, unknown>)["name"]).toBe("name_1");
});
