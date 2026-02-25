import { homedir } from "os";
import { join } from "path";

export const CONFIG_DIR = process.env["MONGO_TOOLS_CONFIG_DIR"] ?? join(homedir(), ".config", "mongo-tools");
export const CLUSTERS_FILE = join(CONFIG_DIR, "clusters.json");

export const SYSTEM_DBS = ["admin", "local", "config"];
