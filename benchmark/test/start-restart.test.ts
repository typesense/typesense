import path from "path";

import { afterAll, expect, test } from "vitest";

import {
  closeDownTypesenseServer,
  env,
  fetchNode,
  restartTypesenseServer,
  restartTypesenseServerFresh,
  startTypesenseServer,
} from "@/global";
import { delay } from "@/utils/base";

afterAll(async () => {
  const res = await closeDownTypesenseServer();

  if (res.isErr()) {
    throw new Error(res.error.message);
  }
});

test("start typesense server", async () => {
  const res = await startTypesenseServer();

  if (res.isErr()) {
    throw new Error(res.error.message);
  }

  const nodes = res.value;

  expect(nodes.length).toBe(3);
  expect(nodes[0]).toMatchObject({
    dataDir: path.join(env.TYPESENSE_WORKING_DIRECTORY, "typesense-data-1"),
    grpc: 8107,
    http: 8108,
  });

  expect(nodes[1]).toMatchObject({
    dataDir: path.join(env.TYPESENSE_WORKING_DIRECTORY, "typesense-data-2"),
    grpc: 7107,
    http: 7108,
  });

  expect(nodes[2]).toMatchObject({
    dataDir: path.join(env.TYPESENSE_WORKING_DIRECTORY, "typesense-data-3"),
    grpc: 9107,
    http: 9108,
  });

  const createCollectionResult = await fetchNode({
    port: 8108,
    endpoint: "collections",
    method: "POST",
    body: {
      name: "test",
      fields: [
        {
          name: "title",
          type: "string",
        },
        {
          name: "content",
          type: "string",
        },
      ],
    },
  });

  if (createCollectionResult.isErr()) {
    throw new Error(createCollectionResult.error.message);
  }

  expect(createCollectionResult.value.name).toBe("test");

  await delay(300);

  const getCollectionResult = await fetchNode({
    port: 8108,
    endpoint: "collections/test",
    method: "GET",
  });

  if (getCollectionResult.isErr()) {
    throw new Error(getCollectionResult.error.message);
  }

  expect(getCollectionResult.value.name).toBe("test");
});

test("restart typesense server", async () => {
  const res = await restartTypesenseServerFresh();

  if (res.isErr()) {
    throw new Error(res.error.message);
  }

  const nodes = res.value;

  expect(nodes.length).toBe(3);
  expect(nodes[0]).toMatchObject({
    dataDir: path.join(env.TYPESENSE_WORKING_DIRECTORY, "typesense-data-1"),
    grpc: 8107,
    http: 8108,
  });

  expect(nodes[1]).toMatchObject({
    dataDir: path.join(env.TYPESENSE_WORKING_DIRECTORY, "typesense-data-2"),
    grpc: 7107,
    http: 7108,
  });

  expect(nodes[2]).toMatchObject({
    dataDir: path.join(env.TYPESENSE_WORKING_DIRECTORY, "typesense-data-3"),
    grpc: 9107,
    http: 9108,
  });

  const getAllCollectionResults = await fetchNode({
    port: 8108,
    endpoint: "collections",
    method: "GET",
  });

  if (getAllCollectionResults.isErr()) {
    throw new Error(getAllCollectionResults.error.message);
  }

  // Should be empty, since we restarted the server fresh
  expect(getAllCollectionResults.value).toStrictEqual([]);
});

test("restart typesense server with skip cleanup", async () => {
  const res = await restartTypesenseServer();

  if (res.isErr()) {
    throw new Error(res.error.message);
  }

  const nodes = res.value;

  expect(nodes.length).toBe(3);
  expect(nodes[0]).toMatchObject({
    dataDir: path.join(env.TYPESENSE_WORKING_DIRECTORY, "typesense-data-1"),
    grpc: 8107,
    http: 8108,
  });

  expect(nodes[1]).toMatchObject({
    dataDir: path.join(env.TYPESENSE_WORKING_DIRECTORY, "typesense-data-2"),
    grpc: 7107,
    http: 7108,
  });

  expect(nodes[2]).toMatchObject({
    dataDir: path.join(env.TYPESENSE_WORKING_DIRECTORY, "typesense-data-3"),
    grpc: 9107,
    http: 9108,
  });

  const createCollectionResult = await fetchNode({
    port: 8108,
    endpoint: "collections",
    method: "POST",
    body: {
      name: "test_after_restart",
      fields: [
        {
          name: "title",
          type: "string",
        },
        {
          name: "content",
          type: "string",
        },
      ],
    },
  });

  if (createCollectionResult.isErr()) {
    throw new Error(createCollectionResult.error.message);
  }

  expect(createCollectionResult.value.name).toBe("test_after_restart");

  await delay(300);

  const getCollectionResult = await fetchNode({
    port: 8108,
    endpoint: "collections/test_after_restart",
    method: "GET",
  });

  if (getCollectionResult.isErr()) {
    throw new Error(getCollectionResult.error.message);
  }

  expect(getCollectionResult.value.name).toBe("test_after_restart");

  const resRestart = await restartTypesenseServer();

  if (resRestart.isErr()) {
    throw new Error(resRestart.error.message);
  }

  const nodesRestart = resRestart.value;

  expect(nodesRestart.length).toBe(3);
  expect(nodesRestart[0]).toMatchObject({
    dataDir: path.join(env.TYPESENSE_WORKING_DIRECTORY, "typesense-data-1"),
    grpc: 8107,
    http: 8108,
  });

  expect(nodesRestart[1]).toMatchObject({
    dataDir: path.join(env.TYPESENSE_WORKING_DIRECTORY, "typesense-data-2"),
    grpc: 7107,
    http: 7108,
  });

  expect(nodes[2]).toMatchObject({
    dataDir: path.join(env.TYPESENSE_WORKING_DIRECTORY, "typesense-data-3"),
    grpc: 9107,
    http: 9108,
  });

  const afterRestartCollection = await fetchNode({
    port: 8108,
    endpoint: "collections/test_after_restart",
    method: "GET",
  });

  if (afterRestartCollection.isErr()) {
    throw new Error(afterRestartCollection.error.message);
  }

  expect(afterRestartCollection.value.name).toBe("test_after_restart");
});
