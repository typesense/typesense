import { describe, expect, it } from "bun:test";
import { mkdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { Phases } from "../src/constants";
import { fetchMultiNode, fetchMultiNodeRequest } from "../src/request";

const COLLECTION_NAME = "snapshot_race_regression_docs";
const SNAPSHOT_EXPORT_PATH = join(process.cwd(), "./data/snapshot/snapshot-race-regression");
const WRITER_COUNT = 3;
const BATCHES_PER_WRITER = 8;
const DOCS_PER_BATCH = 20;
const SNAPSHOT_REQUEST_COUNT = 6;

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

    expect(copyFailures).toHaveLength(0);
    expect(unexpectedResults).toHaveLength(0);
    expect(successCount).toBeGreaterThan(0);

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
