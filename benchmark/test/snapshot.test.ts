import type { CollectionSchema } from "typesense/lib/Typesense/Collection";
import type { CollectionCreateSchema } from "typesense/lib/Typesense/Collections";

import { afterAll, beforeAll, expect, test } from "vitest";

import { closeDownTypesenseServer, env, fetchNode, globalTypesenseManager, startTypesenseServer } from "@/global";
import { exists } from "@/utils/fs";

const collectionName = "snapshot_test";

beforeAll(async () => {
  const res = await startTypesenseServer();

  if (res.isErr()) {
    throw new Error(res.error.message);
  }
});

afterAll(async () => {
  const res = await closeDownTypesenseServer();

  if (res.isErr()) {
    throw new Error(res.error.message);
  }
});

test("Snapshot test", async () => {
  // Setup
  const collectionCreationRes = await fetchNode<CollectionSchema, CollectionCreateSchema>({
    port: 8108,
    endpoint: "collections",
    method: "POST",
    body: {
      name: collectionName,
      fields: [
        {
          name: "title",
          type: "string",
          facet: false,
        },
      ],
    },
  });

  if (collectionCreationRes.isErr()) {
    throw new Error(collectionCreationRes.error.message);
  }

  // Index documents
  const indexDocumentsRes = await fetchNode<CollectionSchema, string>({
    port: 8108,
    endpoint: `/collections/${collectionName}/documents/import`,
    method: "POST",
    body: [{ title: "Hello, world!" }, { title: "Hola, mundo!" }, { title: "Xaire, kosme!" }]
      .map((doc) => JSON.stringify(doc))
      .join("\n"),
  });

  expect(indexDocumentsRes.isOk()).toBe(true);

  const snapshotPath = env.TYPESENSE_SNAPSHOT_PATH ?? `${env.TYPESENSE_WORKING_DIRECTORY}/snapshot`;

  // Take snapshot
  const takeSnapshotRes = await globalTypesenseManager.snapshot(8108);

  expect(takeSnapshotRes.isOk()).toBe(true);

  // Check that the snapshot file exists
  const snapshotDir = await exists(`${snapshotPath}/state/snapshot`);

  expect(snapshotDir.isOk()).toBe(true);
  expect(snapshotDir._unsafeUnwrap()).toBe(true);
});
