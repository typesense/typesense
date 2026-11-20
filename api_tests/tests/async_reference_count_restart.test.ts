import { describe, expect, it } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Phases } from "../src/constants";
import { TypesenseProcessManager, type MultiNodeConfig } from "../src/manager";

// Run with TYPESENSE_BINARY_PATH=/absolute/path/to/typesense-server bun test
// tests/async_reference_count_restart.test.ts from api_tests. Each run keeps its logs
// and observations in a fresh temporary directory, and owns only these three ports.
const NODES: MultiNodeConfig[] = [1, 2, 3].map((id) => ({
  name: `count-node-${id}`,
  port: 18108 + (id - 1) * 1000,
  peerPort: 18107 + (id - 1) * 1000,
  dataDir: `node-${id}`,
  logDir: `node-${id}`,
  analyticsDir: `node-${id}/analytics`,
}));

async function request(node: MultiNodeConfig, path: string, method = "GET", body?: unknown) {
  return fetch(`http://localhost:${node.port}${path}`, {
    method,
    headers: { "X-TYPESENSE-API-KEY": "xyz", "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(30_000),
  });
}

async function json(node: MultiNodeConfig, path: string, method = "GET", body?: unknown) {
  const res = await request(node, path, method, body);
  const text = await res.text();
  if (!res.ok) throw new Error(`${node.name} ${method} ${path}: ${res.status} ${text}`);
  return JSON.parse(text);
}

async function settledStatuses() {
  const deadline = Date.now() + 60_000;
  let statuses: any[] = [];
  let previous = "";
  let stable = 0;
  while (Date.now() < deadline) {
    statuses = await Promise.all(NODES.map((node) => json(node, "/status")));
    const index = statuses[0].committed_index;
    const settled = statuses.every((s) =>
      Number.isInteger(index) && s.committed_index === index &&
      s.known_applied_index === index && s.queued_writes === 0);
    const current = JSON.stringify(statuses.map((s) => [s.committed_index, s.known_applied_index]));
    stable = settled && current === previous ? stable + 1 : 0;
    if (stable >= 3) return statuses;
    previous = current;
    await Bun.sleep(250);
  }
  throw new Error(`Cluster did not settle: ${JSON.stringify(statuses)}`);
}

describe(Phases.NO_PHASE, () => {
  it("keeps rejected async-reference inserts absent across a follower restart and retry", async () => {
    const baseDir = mkdtempSync(join(tmpdir(), "typesense-async-count-"));
    console.log(`Count restart evidence: ${baseDir}`);
    const manager = new TypesenseProcessManager(baseDir);
    const observations: any[] = [];
    const expected: any[] = [];
    async function capture(stage: string, expectedCount: number) {
      const statuses = await settledStatuses();
      for (const [i, node] of NODES.entries()) {
        const summary = await json(node, "/collections/ref_v1");
        const search = await json(node, "/collections/ref_v1/documents/search?q=*&per_page=10");
        const exported = await request(node, "/collections/ref_v1/documents/export");
        if (!exported.ok) throw new Error(`Export failed on ${node.name}: ${exported.status}`);
        const exportIds = (await exported.text()).split("\n").filter((line) => line.trim())
          .map((line) => JSON.parse(line).id).sort();
        const row = { stage, node: node.name, status: statuses[i],
          num_documents: summary.num_documents, found: search.found, export_count: exportIds.length,
          search_ids: search.hits.map((hit: any) => hit.document.id).sort(), export_ids: exportIds };
        observations.push(row);
        expected.push({ ...row, num_documents: expectedCount, found: expectedCount,
          export_count: expectedCount, search_ids: expectedCount === 1 ? ["p1"] : ["p1", "p2"],
          export_ids: expectedCount === 1 ? ["p1"] : ["p1", "p2"] });
        console.log(JSON.stringify(row));
      }
      writeFileSync(join(baseDir, "observations.json"), JSON.stringify(observations, null, 2));
    }
    try {
      await manager.startMultiNode(NODES);
      const statuses = await settledStatuses();
      const leader = NODES[statuses.findIndex((s) => s.state === "LEADER")]!;
      expect(leader).toBeDefined();
      const follower = NODES.find((node) => node !== leader)!;
      await json(leader, "/collections", "POST", {
        name: "ref_v1", fields: [{ name: "ref_key", type: "string" }],
      });
      await json(leader, "/aliases/ref", "PUT", { collection_name: "ref_v1" });
      await json(leader, "/collections", "POST", {
        name: "items", fields: [{ name: "ref_key", type: "string", reference: "ref.ref_key",
          async_reference: true, cascade_delete: false }],
      });
      await json(leader, "/collections/ref_v1/documents", "POST", { id: "p1", ref_key: "key1" });
      await json(leader, "/collections/items/documents", "POST", { id: "c1", ref_key: "key1" });
      await capture("seed", 1);
      const rejected = await request(leader, "/collections/ref_v1/documents", "POST", {
        id: "p2", ref_key: "key1",
      });
      expect(rejected.status).toBe(400);
      expect((await rejected.json()).message).toBe(
        "Error while updating async reference field `ref_key` of collection `items`: " +
        "The value `key1` of the field `ref_key` is not unique in `ref_v1` collection.");
      await capture("rejected", 1);

      // Snapshot the settled state before restart so reload can expose an orphaned
      // persisted record without replay of the original rejected request masking it.
      await manager.createSnapshot(follower.port, join(baseDir, "snapshot"));
      await settledStatuses();
      await manager.stopServer(follower.name);
      await manager.startClusterNode(follower);
      await capture("follower-restarted", 1);
      await json(leader, "/collections/ref_v1/documents?action=upsert", "POST", {
        id: "p2", ref_key: "key2",
      });
      await capture("retried", 2);

      // Assert after all stages so a buggy binary still records the restart and retry evidence.
      expect(observations).toEqual(expected);
    } finally {
      await manager.shutdown();
    }
  }, 180_000);
});
