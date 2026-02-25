# Mongo Tools

MongoDB cluster management mini-tool.
Yep, i'm lazy asf.

## Install

```bash
bun install
bun run build
bun run install-global
```

Then run `mongo-tools` from anywhere.

You'll need:
- A MongoDB connection URI for each cluster you want to manage
- Clusters get saved locally in `~/.config/mongo-tools/` so you only set them up once

## Dev

```bash
bun start  # Run directly without building
```

## What it does

- **Clone** — copy a database within the same or another cluster
- **Download** — export a database to local files
- **Upload** — import a database from local files
- **Drop** — permanently delete a database

## Tech

- Bun + TypeScript
- Connects directly via the MongoDB driver
- Interactive TUI with `@clack/prompts`
- Everything stays local, nothing sent anywhere else
