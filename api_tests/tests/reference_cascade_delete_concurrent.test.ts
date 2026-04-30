import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { Phases } from "../src/constants";
import { checkCommitedIndex, fetchMultiNode, fetchMultiNodeRequest } from "../src/request";

const NODE_IDS = [1, 2, 3] as const;
type NodeId = typeof NODE_IDS[number];

const COLLECTIONS = {
  products: "cascade_delete_products",
  orders: "cascade_delete_orders",
} as const;

const CATEGORY_COUNT = 5;
const NUM_PRODUCTS = 100;
const NUM_RESTAURANTS = 5;
const IMPORT_BATCH_SIZE = 200;
const ROUNDS = 10;
const ORDER_IMPORTER_WORKERS = 3;
const PRODUCT_UPSERTER_WORKERS = 1;
const PRODUCT_DELETER_WORKERS = 2;

const CATEGORIES = Array.from({ length: CATEGORY_COUNT }, (_, index) => `cat-${index}`);

const CollectionResponse = z.object({
  name: z.string(),
  num_documents: z.number(),
});

const DeleteDocumentsResponse = z.object({
  num_deleted: z.number(),
});

const StatsResponse = z.object({
  pending_write_batches: z.number(),
});

const ClusterStateSchema = z.object({
  products: z.number(),
  orders: z.number(),
});

type ClusterState = z.infer<typeof ClusterStateSchema>;

type ProductDocument = {
  id: string;
  variant_pack_uuid: string;
  category: string;
};

type OrderDocument = {
  id: string;
  restaurant_uuid: string;
  variant_pack_uuid: string;
  category: string;
};

type ExecutionStats = {
  importedDocs: number;
  productUpsertedDocs: number;
  productDeleteRequests: number;
  deletedProductDocs: number;
};

function portForNode(node: NodeId) {
  return `${node + 4}108`;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function categoryForProduct(productIndex: number) {
  return CATEGORIES[productIndex % CATEGORY_COUNT]!;
}

function categoryForRound(round: number) {
  return CATEGORIES[round % CATEGORY_COUNT]!;
}

function randomItem<T>(items: readonly T[]) {
  return items[Math.floor(Math.random() * items.length)]!;
}

function buildProduct(productIndex: number): ProductDocument {
  return {
    id: `cascade-product-${productIndex}`,
    variant_pack_uuid: `cascade-vpuuid-${productIndex}`,
    category: categoryForProduct(productIndex),
  };
}

function buildOrder(round: number, index: number): OrderDocument {
  const productIndex = (round * IMPORT_BATCH_SIZE + index) % NUM_PRODUCTS;

  return {
    id: `cascade-order-${round}-${index}`,
    restaurant_uuid: `rest-${(round + index) % NUM_RESTAURANTS}`,
    variant_pack_uuid: `cascade-vpuuid-${productIndex}`,
    category: categoryForProduct(productIndex),
  };
}

function jsonl(docs: Array<Record<string, string>>) {
  return docs.map((doc) => JSON.stringify(doc)).join("\n");
}

function parseImportSuccessCount(payload: string) {
  const lines = payload.trim();
  if (lines.length === 0) {
    return 0;
  }

  return lines.split("\n").reduce((count, line) => {
    const result = z.object({ success: z.boolean() }).safeParse(JSON.parse(line));
    if (!result.success) {
      throw new Error(`Unexpected import response line: ${line}`);
    }

    return result.data.success ? count + 1 : count;
  }, 0);
}

function emptyExecutionStats(): ExecutionStats {
  return {
    importedDocs: 0,
    productUpsertedDocs: 0,
    productDeleteRequests: 0,
    deletedProductDocs: 0,
  };
}

function addExecutionStats(current: ExecutionStats, next: ExecutionStats) {
  current.importedDocs += next.importedDocs;
  current.productUpsertedDocs += next.productUpsertedDocs;
  current.productDeleteRequests += next.productDeleteRequests;
  current.deletedProductDocs += next.deletedProductDocs;
}

async function fetchStats(node: NodeId) {
  const res = await fetch(`http://localhost:${portForNode(node)}/stats.json`, {
    headers: {
      "X-TYPESENSE-API-KEY": "xyz",
    },
    signal: AbortSignal.timeout(10000),
  });

  expect(res.ok).toBe(true);
  return StatsResponse.parse(await res.json());
}

async function waitForPendingWrites(timeoutMs = 15000) {
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    const [isCommitted, stats] = await Promise.all([
      checkCommitedIndex(),
      Promise.all(NODE_IDS.map((node) => fetchStats(node))),
    ]);
    if (isCommitted && stats.every((entry) => entry.pending_write_batches === 0)) {
      return;
    }

    await sleep(100);
  }

  throw new Error("Timed out waiting for committed index sync and pending write batches to drain");
}

async function createCollection(node: NodeId, schema: Record<string, unknown>) {
  const res = await fetchMultiNode(node, "/collections", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(schema),
  });

  expect(res.ok).toBe(true);
}

async function createCascadeCollections() {
  await createCollection(1, {
    name: COLLECTIONS.products,
    fields: [
      { name: "id", type: "string" },
      { name: "variant_pack_uuid", type: "string" },
      { name: "category", type: "string", facet: true },
    ],
  });

  await createCollection(1, {
    name: COLLECTIONS.orders,
    fields: [
      { name: "id", type: "string" },
      { name: "restaurant_uuid", type: "string", facet: true },
      {
        name: "variant_pack_uuid",
        type: "string",
        reference: `${COLLECTIONS.products}.variant_pack_uuid`,
      },
    ],
  });
}

async function upsertProducts(node: NodeId) {
  const products = Array.from({ length: NUM_PRODUCTS }, (_, index) => buildProduct(index));
  const res = await fetchMultiNodeRequest(
    node,
    `/collections/${COLLECTIONS.products}/documents/import?action=upsert`,
    {
      method: "POST",
      headers: {
        "Content-Type": "text/plain",
      },
      body: jsonl(products),
    },
  );

  expect(res.ok).toBe(true);
  return parseImportSuccessCount(await res.text());
}

async function importOrders(node: NodeId, round: number) {
  const orders = Array.from({ length: IMPORT_BATCH_SIZE }, (_, index) => buildOrder(round, index));
  const payload = orders.map(({ id, restaurant_uuid, variant_pack_uuid }) => ({
    id,
    restaurant_uuid,
    variant_pack_uuid,
  }));

  const res = await fetchMultiNodeRequest(
    node,
    `/collections/${COLLECTIONS.orders}/documents/import?action=upsert`,
    {
      method: "POST",
      headers: {
        "Content-Type": "text/plain",
      },
      body: jsonl(payload),
    },
  );

  expect(res.ok).toBe(true);
  return parseImportSuccessCount(await res.text());
}

async function deleteProductsByCategory(node: NodeId, category: string) {
  const params = new URLSearchParams({
    filter_by: `category:=\`${category}\``,
  });

  const res = await fetchMultiNodeRequest(
    node,
    `/collections/${COLLECTIONS.products}/documents?${params.toString()}`,
    {
      method: "DELETE",
    },
  );

  expect(res.ok).toBe(true);
  return DeleteDocumentsResponse.parse(await res.json()).num_deleted;
}

async function collectionCount(node: NodeId, name: string) {
  const res = await fetchMultiNodeRequest(node, `/collections/${name}`);
  expect(res.ok).toBe(true);
  return CollectionResponse.parse(await res.json()).num_documents;
}

async function verifyClusterConsistency() {
  await waitForPendingWrites();

  let counts = await Promise.all(NODE_IDS.map((node) => collectionCount(node, COLLECTIONS.products)));
  expect(counts.length).toBe(3);
  // All the nodes must have same count.
  let set = new Set(counts);
  if (set.size > 1) {
    console.log(counts);
  }
  expect(set.size).toBe(1);


  counts = await Promise.all(NODE_IDS.map((node) => collectionCount(node, COLLECTIONS.orders)));
  expect(counts.length).toBe(3);
  // All the nodes must have same count.
  set = new Set(counts);
  if (set.size > 1) {
    console.log(counts);
  }
  expect(set.size).toBe(1);
}

async function runOrderImporter(workerId: number) {
  for (let round = workerId; round < ROUNDS; round += ORDER_IMPORTER_WORKERS) {
    const node = NODE_IDS[(round + workerId + 1) % NODE_IDS.length]!;
    await importOrders(node, round);
  }
}

async function runProductUpserter(workerId: number) {
  for (let round = workerId; round < ROUNDS; round += PRODUCT_UPSERTER_WORKERS) {
    const node = NODE_IDS[(round + workerId) % NODE_IDS.length]!;
    await upsertProducts(node);
  }
}

async function runProductDeleter(workerId: number) {
  for (let round = workerId; round < ROUNDS; round += PRODUCT_DELETER_WORKERS) {
    const node = randomItem(NODE_IDS);
    const deletedCategory = randomItem(CATEGORIES);
    await deleteProductsByCategory(node, deletedCategory);
  }
}

describe(Phases.MULTI_FRESH, () => {
  it("keeps cascade deletes consistent under concurrent imports, upserts, and deletes", async () => {
    await createCascadeCollections();
    await waitForPendingWrites();

    const seededProducts = await upsertProducts(1);
    expect(seededProducts).toBe(NUM_PRODUCTS);
    await waitForPendingWrites();

    await Promise.all([
      ...Array.from({ length: ORDER_IMPORTER_WORKERS }, (_, workerId) => runOrderImporter(workerId)),
      ...Array.from({ length: PRODUCT_UPSERTER_WORKERS }, (_, workerId) => runProductUpserter(workerId)),
      ...Array.from({ length: PRODUCT_DELETER_WORKERS }, (_, workerId) => runProductDeleter(workerId)),
    ]);

    await verifyClusterConsistency();
  });
});

describe(Phases.MULTI_RESTARTED, () => {
  it("preserves the cascade test state after restart", async () => {
    await verifyClusterConsistency();
  });
});

describe(Phases.MULTI_SNAPSHOT, () => {
  it("preserves the cascade test state after snapshot restore", async () => {
    await verifyClusterConsistency();
  });
});
