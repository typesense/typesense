import { describe, expect, it } from "bun:test";
import { existsSync, mkdirSync, readdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { Phases } from "../src/constants";
import { TypesenseProcessManager } from "../src/manager";
import { fetchMultiNode, fetchMultiNodeRequest, fetchSingleNode } from "../src/request";

const COLLECTION_NAME = "snapshot_race_regression_docs";
const SNAPSHOT_EXPORT_PATH = join(process.cwd(), "./data/snapshot/snapshot-race-regression");
const WRITER_COUNT = 3;
const BATCHES_PER_WRITER = 8;
const DOCS_PER_BATCH = 20;
const SNAPSHOT_REQUEST_COUNT = 12;
const SNAPSHOT_IN_PROGRESS_ERROR = "Another snapshot is in progress.";
const DOCUMENT_PAYLOAD = "snapshot-race-payload".repeat(128);

type SnapshotResult = {
  status: number;
  body: {
    success?: boolean;
    error?: string;
  } | null;
  text: string;
};

async function createRegressionCollection() {
  const res = await fetchMultiNode(1, "/collections", {
    method: "POST",
    body: JSON.stringify({
      name: COLLECTION_NAME,
      fields: [
        { name: "id", type: "string" },
        { name: "title", type: "string" },
        { name: "payload", type: "string" },
        { name: "batch", type: "int32" },
        { name: "writer", type: "int32" },
      ],
    }),
  });

  expect(res.ok).toBe(true);
}

function buildImportPayload(writer: number, batch: number) {
  const docs: string[] = [];

  for (let doc = 0; doc < DOCS_PER_BATCH; doc++) {
    docs.push(JSON.stringify({
      id: `race-${writer}-${batch}-${doc}`,
      title: `snapshot-race-${writer}-${batch}-${doc}`,
      payload: DOCUMENT_PAYLOAD,
      batch,
      writer,
    }));
  }

  return docs.join("\n");
}

async function runWriter(writer: number) {
  for (let batch = 0; batch < BATCHES_PER_WRITER; batch++) {
    const res = await fetchMultiNodeRequest(
      1,
      `/collections/${COLLECTION_NAME}/documents/import?action=upsert`,
      {
        method: "POST",
        body: buildImportPayload(writer, batch),
      },
    );

    expect(res.ok).toBe(true);

    const lines = (await res.text())
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line) as { success?: boolean; error?: string });

    expect(lines.length).toBe(DOCS_PER_BATCH);
    expect(lines.every((line) => line.success === true)).toBe(true);
  }
}

async function requestSnapshot(): Promise<SnapshotResult> {
  const res = await fetch(
    `http://localhost:5108/operations/snapshot?snapshot_path=${encodeURIComponent(SNAPSHOT_EXPORT_PATH)}`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-TYPESENSE-API-KEY": "xyz",
      },
      signal: AbortSignal.timeout(60_000),
    },
  );

  const text = await res.text();
  let body: SnapshotResult["body"] = null;

  try {
    body = JSON.parse(text);
  } catch {}

  return {
    status: res.status,
    body,
    text,
  };
}

async function getDocument(id: string) {
  const res = await fetchMultiNode(2, `/collections/${COLLECTION_NAME}/documents/${id}`, {
    method: "GET",
  });

  expect(res.ok).toBe(true);
  return res.json() as Promise<{ id: string; writer: number; batch: number }>;
}

function verifyExternalSnapshotLayout() {
  const statePath = join(SNAPSHOT_EXPORT_PATH, "state");
  const snapshotPath = join(statePath, "snapshot");
  const metaPath = join(statePath, "meta");

  expect(existsSync(snapshotPath)).toBe(true);
  expect(existsSync(metaPath)).toBe(true);
  expect(existsSync(join(snapshotPath, "snapshot"))).toBe(false);

  const snapshotDirectories = readdirSync(snapshotPath, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && entry.name.startsWith("snapshot_"))
    .map((entry) => entry.name);

  expect(snapshotDirectories.length).toBeGreaterThan(0);
  expect(readdirSync(metaPath).length).toBeGreaterThan(0);

  const latestSnapshotPath = join(snapshotPath, snapshotDirectories.sort().at(-1)!);
  expect(existsSync(join(latestSnapshotPath, "db_snapshot", "CURRENT"))).toBe(true);
}

describe(Phases.MULTI_FRESH, () => {
  it("does not return Copy failed during concurrent external snapshot requests", async () => {
    rmSync(SNAPSHOT_EXPORT_PATH, { recursive: true, force: true });
    mkdirSync(SNAPSHOT_EXPORT_PATH, { recursive: true });

    await createRegressionCollection();

    const writerPromises = Array.from({ length: WRITER_COUNT }, (_, writer) => runWriter(writer));

    await Bun.sleep(250);

    const snapshotResultsPromise = Promise.all(
      Array.from({ length: SNAPSHOT_REQUEST_COUNT }, () => requestSnapshot()),
    );

    await Promise.all(writerPromises);
    const snapshotResults = await snapshotResultsPromise;

    const unexpectedResults = snapshotResults.filter((result) => ![201, 409].includes(result.status));
    const copyFailures = snapshotResults.filter((result) =>
      result.text.includes("Copy failed") || result.body?.error === "Copy failed.",
    );
    const successCount = snapshotResults.filter((result) => result.status === 201).length;
    const rejectedResults = snapshotResults.filter((result) => result.status === 409);

    expect(copyFailures).toHaveLength(0);
    expect(unexpectedResults).toHaveLength(0);
    expect(successCount).toBeGreaterThan(0);
    expect(rejectedResults.length).toBeGreaterThan(0);
    expect(rejectedResults.every((result) => result.text.includes(SNAPSHOT_IN_PROGRESS_ERROR))).toBe(true);

    verifyExternalSnapshotLayout();

    const sampleDoc = await getDocument("race-0-0-0");
    expect(sampleDoc.id).toBe("race-0-0-0");
    expect(sampleDoc.writer).toBe(0);
  });
});

describe(Phases.MULTI_SNAPSHOT, () => {
  it("preserves documents written during the snapshot race regression workload", async () => {
    const firstDoc = await getDocument("race-0-0-0");
    expect(firstDoc.id).toBe("race-0-0-0");

    const lastDoc = await getDocument(`race-${WRITER_COUNT - 1}-${BATCHES_PER_WRITER - 1}-${DOCS_PER_BATCH - 1}`);
    expect(lastDoc.id).toBe(`race-${WRITER_COUNT - 1}-${BATCHES_PER_WRITER - 1}-${DOCS_PER_BATCH - 1}`);
  });
});

describe(Phases.NO_PHASE, () => {
  it("restores a server from the external snapshot layout", async () => {
    const restoreBasePath = join(process.cwd(), "./data/snapshot-external-restore");
    const restoreSnapshotPath = join(restoreBasePath, "export");
    const restoreCollectionName = "snapshot_external_restore_docs";
    const restorePort = 9108;
    const restorePeeringPort = 9107;
    const manager = new TypesenseProcessManager(restoreBasePath);

    rmSync(restoreBasePath, { recursive: true, force: true });
    mkdirSync(restoreBasePath, { recursive: true });

    try {
      await manager.startSingleNode("source", restorePort, restorePeeringPort, "snapshot-restore-source");

      const createCollectionRes = await fetchSingleNode(
        "/collections",
        {
          method: "POST",
          body: JSON.stringify({
            name: restoreCollectionName,
            fields: [{ name: "title", type: "string" }],
          }),
        },
        restorePort,
      );
      expect(createCollectionRes.status).toBe(201);

      const createDocumentRes = await fetchSingleNode(
        `/collections/${restoreCollectionName}/documents`,
        {
          method: "POST",
          body: JSON.stringify({ id: "restored-document", title: "survives external restore" }),
        },
        restorePort,
      );
      expect(createDocumentRes.status).toBe(201);

      await manager.createSnapshot(restorePort, restoreSnapshotPath);
      await manager.shutdown();

      await manager.startSingleNode("export", restorePort, restorePeeringPort, "snapshot-restore-target");

      const restoredDocumentRes = await fetchSingleNode(
        `/collections/${restoreCollectionName}/documents/restored-document`,
        undefined,
        restorePort,
      );
      expect(restoredDocumentRes.status).toBe(200);
      expect(await restoredDocumentRes.json()).toMatchObject({
        id: "restored-document",
        title: "survives external restore",
      });
    } finally {
      await manager.shutdown();
      rmSync(restoreBasePath, { recursive: true, force: true });
    }
  }, { timeout: 120_000 });
});
