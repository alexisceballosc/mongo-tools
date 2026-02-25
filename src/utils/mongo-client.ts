import { MongoClient as NativeClient } from "mongodb";
import type { Document, IndexDescription } from "mongodb";
import { EJSON } from "bson";
import { createReadStream, createWriteStream, existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "fs";
import { createInterface } from "readline";
import { join } from "path";
import { SYSTEM_DBS } from "../config/constants.js";
import type { DbInfo, CollectionInfo, DbStats, ExportResult, ImportResult } from "../config/types.js";

const BATCH_SIZE = 500;

export class MongoClient {
  private client: NativeClient | null = null;

  async connect(uri: string): Promise<void> {
    this.client = new NativeClient(uri, { serverSelectionTimeoutMS: 8000 });
    await this.client.connect();
  }

  async disconnect(): Promise<void> {
    await this.client?.close();
    this.client = null;
  }

  private get db() {
    if (!this.client) throw new Error("Not connected to a cluster.");
    return this.client;
  }

  async listDatabases(): Promise<DbInfo[]> {
    const result = await this.db.db("admin").admin().listDatabases();
    return result.databases
      .filter((d) => !SYSTEM_DBS.includes(d.name))
      .map((d) => ({ name: d.name, sizeOnDisk: d.sizeOnDisk }));
  }

  async listCollections(dbName: string): Promise<CollectionInfo[]> {
    const db = this.db.db(dbName);
    const collections = await db.listCollections().toArray();
    return Promise.all(
      collections.map(async (col) => ({
        name: col.name,
        count: await db.collection(col.name).countDocuments(),
      }))
    );
  }

  async getDbStats(dbName: string): Promise<DbStats> {
    const collections = await this.listCollections(dbName);
    const totalDocuments = collections.reduce((sum, c) => sum + c.count, 0);
    return { name: dbName, collections, totalDocuments };
  }

  async cloneDb(
    sourceDb: string,
    targetDb: string,
    onCollection?: (name: string, index: number, total: number) => void
  ): Promise<void> {
    const source = this.db.db(sourceDb);
    const target = this.db.db(targetDb);
    const collections = await source.listCollections().toArray();

    for (let i = 0; i < collections.length; i++) {
      const col = collections[i];
      onCollection?.(col.name, i + 1, collections.length);

      const cursor = source.collection(col.name).find({});
      let batch: Document[] = [];
      for await (const doc of cursor) {
        batch.push(doc);
        if (batch.length >= BATCH_SIZE) {
          await target.collection(col.name).insertMany(batch);
          batch = [];
        }
      }
      if (batch.length > 0) {
        await target.collection(col.name).insertMany(batch);
      }

      const indexes = await source.collection(col.name).indexes();
      const userIndexes = indexes
        .filter((idx) => idx.name !== "_id_")
        .map(({ v: _v, ns: _ns, ...rest }: Document) => rest as IndexDescription);
      if (userIndexes.length > 0) {
        await target.collection(col.name).createIndexes(userIndexes);
      }
    }
  }

  async cloneDbToClient(
    sourceDb: string,
    targetClient: MongoClient,
    targetDb: string,
    onCollection?: (name: string, index: number, total: number) => void
  ): Promise<ImportResult> {
    const source = this.db.db(sourceDb);
    const target = targetClient.db.db(targetDb);
    const collections = await source.listCollections().toArray();
    let totalDocuments = 0;

    for (let i = 0; i < collections.length; i++) {
      const col = collections[i];
      onCollection?.(col.name, i + 1, collections.length);

      const cursor = source.collection(col.name).find({});
      let batch: Document[] = [];
      for await (const doc of cursor) {
        batch.push(doc);
        if (batch.length >= BATCH_SIZE) {
          await target.collection(col.name).insertMany(batch);
          totalDocuments += batch.length;
          batch = [];
        }
      }
      if (batch.length > 0) {
        await target.collection(col.name).insertMany(batch);
        totalDocuments += batch.length;
      }

      const indexes = await source.collection(col.name).indexes();
      const userIndexes = indexes
        .filter((idx) => idx.name !== "_id_")
        .map(({ v: _v, ns: _ns, ...rest }: Document) => rest as IndexDescription);
      if (userIndexes.length > 0) {
        await target.collection(col.name).createIndexes(userIndexes);
      }
    }

    return { collections: collections.length, documents: totalDocuments };
  }

  async dropDb(dbName: string): Promise<void> {
    await this.db.db(dbName).dropDatabase();
  }

  async exportDb(
    dbName: string,
    outDir: string,
    onCollection?: (name: string, index: number, total: number) => void
  ): Promise<ExportResult> {
    mkdirSync(outDir, { recursive: true });

    const db = this.db.db(dbName);
    const collections = await db.listCollections().toArray();
    let totalDocuments = 0;

    for (let i = 0; i < collections.length; i++) {
      const col = collections[i];
      onCollection?.(col.name, i + 1, collections.length);

      const stream = createWriteStream(join(outDir, `${col.name}.jsonl`), { encoding: "utf8" });
      const cursor = db.collection(col.name).find({});
      for await (const doc of cursor) {
        stream.write(EJSON.stringify(doc) + "\n");
        totalDocuments++;
      }
      await new Promise<void>((resolve, reject) => {
        stream.end((err?: Error | null) => (err ? reject(err) : resolve()));
      });

      const indexes = await db.collection(col.name).indexes();
      const userIndexes = indexes.filter((idx) => idx.name !== "_id_");
      writeFileSync(join(outDir, `${col.name}.indexes.json`), JSON.stringify(userIndexes, null, 2), "utf-8");
    }

    return { collections: collections.length, documents: totalDocuments, path: outDir };
  }

  async importDb(
    dbName: string,
    inDir: string,
    onCollection?: (name: string, index: number, total: number) => void
  ): Promise<ImportResult> {
    if (!existsSync(inDir)) throw new Error(`Directory not found: ${inDir}`);

    const db = this.db.db(dbName);
    const files = readdirSync(inDir).filter((f) => f.endsWith(".jsonl"));
    let totalDocuments = 0;

    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      const colName = file.replace(/\.jsonl$/, "");
      onCollection?.(colName, i + 1, files.length);

      const rl = createInterface({
        input: createReadStream(join(inDir, file), { encoding: "utf8" }),
        crlfDelay: Infinity,
      });
      let batch: Document[] = [];
      for await (const line of rl) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        batch.push(EJSON.parse(trimmed) as Document);
        if (batch.length >= BATCH_SIZE) {
          await db.collection(colName).insertMany(batch);
          totalDocuments += batch.length;
          batch = [];
        }
      }
      if (batch.length > 0) {
        await db.collection(colName).insertMany(batch);
        totalDocuments += batch.length;
      }

      const indexFile = join(inDir, `${colName}.indexes.json`);
      if (existsSync(indexFile)) {
        const rawIndexes = JSON.parse(readFileSync(indexFile, "utf-8")) as Document[];
        const userIndexes = rawIndexes
          .filter((idx) => idx["name"] !== "_id_")
          .map(({ v: _v, ns: _ns, ...rest }: Document) => rest as IndexDescription);
        if (userIndexes.length > 0) {
          await db.collection(colName).createIndexes(userIndexes);
        }
      }
    }

    return { collections: files.length, documents: totalDocuments };
  }
}
