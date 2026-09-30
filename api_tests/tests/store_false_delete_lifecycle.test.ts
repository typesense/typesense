import { afterAll, beforeAll, describe, expect, it, setDefaultTimeout } from "bun:test";
import { mkdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { TypesenseProcessManager, type MultiNodeConfig } from "../src/manager";

setDefaultTimeout(5 * 60 * 1000);

const BASE_DIR = join(process.cwd(), "./data/store-false-delete-lifecycle");
const API_KEY = "xyz";
const COLLECTION = "store_false_delete_lifecycle";
const SINGLE_PORT = 18108;
const SINGLE_PEER_PORT = 18107;
const CLUSTER_NODES: MultiNodeConfig[] = [
  {
    name: "store-false-delete-node1",
    port: 18208,
    peerPort: 18207,
    dataDir: "cluster-data-1",
    logDir: "cluster-log-1",
    analyticsDir: "cluster-data-1/analytics_db",
  },
  {
    name: "store-false-delete-node2",
    port: 18308,
    peerPort: 18307,
    dataDir: "cluster-data-2",
    logDir: "cluster-log-2",
    analyticsDir: "cluster-data-2/analytics_db",
  },
  {
    name: "store-false-delete-node3",
    port: 18408,
    peerPort: 18407,
    dataDir: "cluster-data-3",
    logDir: "cluster-log-3",
    analyticsDir: "cluster-data-3/analytics_db",
  },
];

type SearchResponse = {
  found: number;
  found_docs?: number;
  hits?: Array<{ document: { id: string } }>;
  grouped_hits?: Array<{ hits: Array<{ document: { id: string } }> }>;
  facet_counts?: Array<{ counts: Array<{ value: string; highlighted?: string; count: number }> }>;
};

type StatusResponse = {
  committed_index: number;
  known_applied_index: number;
  applying_index: number;
};

let manager: TypesenseProcessManager;

beforeAll(() => {
  rmSync(BASE_DIR, { recursive: true, force: true });
  mkdirSync(BASE_DIR, { recursive: true });
  manager = new TypesenseProcessManager(BASE_DIR, process.env.TYPESENSE_BINARY_PATH!);
});

afterAll(async () => {
  if (manager) {
    await manager.shutdown();
  }
  rmSync(BASE_DIR, { recursive: true, force: true });
});

async function request(port: number, path: string, init: RequestInit = {}): Promise<Response> {
  return fetch(`http://localhost:${port}${path}`, {
    ...init,
    headers: {
      ...init.headers,
      "X-TYPESENSE-API-KEY": API_KEY,
    },
    signal: AbortSignal.timeout(30_000),
  });
}

async function waitForCondition(label: string, predicate: () => Promise<boolean>, timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      if (await predicate()) {
        return;
      }
    } catch {}
    await Bun.sleep(250);
  }
  throw new Error(`Timed out waiting for ${label}`);
}

async function waitForAppliedIndexes(ports: number[]) {
  await waitForCondition("Raft indexes to converge", async () => {
    const responses = await Promise.all(ports.map((port) => request(port, "/status")));
    if (responses.some((response) => !response.ok)) {
      return false;
    }

    const statuses = await Promise.all(responses.map((response) => response.json() as Promise<StatusResponse>));
    const committedIndex = statuses[0]?.committed_index;
    return committedIndex !== undefined && statuses.every((status) => {
      const appliedIndex = status.applying_index === 0 ? status.known_applied_index : status.applying_index;
      return status.committed_index === committedIndex && appliedIndex === committedIndex;
    });
  });
}

async function createFixture(port: number) {
  let response = await request(port, "/collections", {
    method: "POST",
    body: JSON.stringify({
      name: COLLECTION,
      fields: [
        { name: "title", type: "string" },
        { name: "hidden", type: "string", facet: true, store: false },
        { name: "kind", type: "string", facet: true },
        { name: "group", type: "string", facet: true },
      ],
    }),
  });
  expect(response.status).toBe(201);

  const documents = [
    { id: "deleted", title: "removed document", hidden: "remove", kind: "removed", group: "gone" },
    { id: "live-a", title: "first live document", hidden: "keep", kind: "live", group: "survivors" },
    { id: "live-b", title: "second live document", hidden: "keep", kind: "live", group: "survivors" },
  ];
  for (const document of documents) {
    response = await request(port, `/collections/${COLLECTION}/documents`, {
      method: "POST",
      body: JSON.stringify(document),
    });
    expect(response.status).toBe(201);
  }
}

async function deleteByHiddenField(port: number): Promise<number> {
  const params = new URLSearchParams({ filter_by: "hidden:=remove" });
  const response = await request(port, `/collections/${COLLECTION}/documents?${params}`, { method: "DELETE" });
  expect(response.ok).toBe(true);
  const body = await response.json() as { num_deleted: number };
  return body.num_deleted;
}

async function verifyDeletedState(port: number) {
  let response = await request(port, `/collections/${COLLECTION}/documents/deleted`);
  expect(response.status).toBe(404);

  let params = new URLSearchParams({
    q: "*",
    filter_by: "hidden:=remove",
    facet_by: "hidden",
    group_by: "hidden",
    group_limit: "3",
  });
  response = await request(port, `/collections/${COLLECTION}/documents/search?${params}`);
  expect(response.ok).toBe(true);
  const deletedSearch = await response.json() as SearchResponse;
  expect(deletedSearch.found).toBe(0);
  expect(deletedSearch.found_docs ?? 0).toBe(0);
  expect(deletedSearch.hits ?? []).toHaveLength(0);
  expect(deletedSearch.grouped_hits ?? []).toHaveLength(0);
  expect((deletedSearch.facet_counts ?? []).flatMap((facet) => facet.counts)
    .find((count) => count.value === "remove")).toBeUndefined();

  params = new URLSearchParams({ q: "*", facet_by: "kind", per_page: "10" });
  response = await request(port, `/collections/${COLLECTION}/documents/search?${params}`);
  expect(response.ok).toBe(true);
  const liveSearch = await response.json() as SearchResponse;
  expect(liveSearch.found).toBe(2);
  expect((liveSearch.hits ?? []).map((hit) => hit.document.id).sort()).toEqual(["live-a", "live-b"]);
  expect(liveSearch.facet_counts?.[0]?.counts).toEqual([{ value: "live", highlighted: "live", count: 2 }]);

  params = new URLSearchParams({ q: "*", group_by: "group", group_limit: "3" });
  response = await request(port, `/collections/${COLLECTION}/documents/search?${params}`);
  expect(response.ok).toBe(true);
  const groupedSearch = await response.json() as SearchResponse;
  const groupedIds = (groupedSearch.grouped_hits ?? [])
    .flatMap((group) => group.hits.map((hit) => hit.document.id))
    .sort();
  expect(groupedIds).toEqual(["live-a", "live-b"]);
}

describe("store:false delete lifecycle", () => {
  it("keeps deleted IDs masked across clean restart, log replay, and snapshot restart", async () => {
    await manager.startSingleNode("single-data", SINGLE_PORT, SINGLE_PEER_PORT, "store-false-delete-single");
    await createFixture(SINGLE_PORT);
    expect(await deleteByHiddenField(SINGLE_PORT)).toBe(1);
    await verifyDeletedState(SINGLE_PORT);

    await manager.stopServer("store-false-delete-single");
    await manager.startSingleNode("single-data", SINGLE_PORT, SINGLE_PEER_PORT, "store-false-delete-single");
    await verifyDeletedState(SINGLE_PORT);

    await manager.createSnapshot(SINGLE_PORT, join(BASE_DIR, "single-external-snapshot"));
    await manager.stopServer("store-false-delete-single");
    await manager.startSingleNode("single-data", SINGLE_PORT, SINGLE_PEER_PORT, "store-false-delete-single");
    await verifyDeletedState(SINGLE_PORT);
    expect(await deleteByHiddenField(SINGLE_PORT)).toBe(0);
    await manager.stopServer("store-false-delete-single");

    await manager.startMultiNode(CLUSTER_NODES, join(BASE_DIR, "cluster-nodes"));
    const [leader, replayFollower, snapshotFollower] = CLUSTER_NODES;
    await createFixture(leader!.port);
    await waitForAppliedIndexes(CLUSTER_NODES.map((node) => node.port));

    await manager.stopServer(replayFollower!.name);
    expect(await deleteByHiddenField(leader!.port)).toBe(1);
    await waitForAppliedIndexes([leader!.port, snapshotFollower!.port]);
    await verifyDeletedState(leader!.port);
    await verifyDeletedState(snapshotFollower!.port);

    await manager.startClusterNode(replayFollower!, join(BASE_DIR, "cluster-nodes"));
    await waitForAppliedIndexes(CLUSTER_NODES.map((node) => node.port));
    await verifyDeletedState(replayFollower!.port);

    await manager.createSnapshot(leader!.port, join(BASE_DIR, "cluster-external-snapshot"));
    await manager.stopServer(snapshotFollower!.name);
    await manager.startClusterNode(snapshotFollower!, join(BASE_DIR, "cluster-nodes"));
    await waitForAppliedIndexes(CLUSTER_NODES.map((node) => node.port));
    await Promise.all(CLUSTER_NODES.map((node) => verifyDeletedState(node.port)));
    expect(await deleteByHiddenField(leader!.port)).toBe(0);
  });
});
