import { afterAll, beforeAll, describe, expect, it, setDefaultTimeout } from "bun:test";
import { cpSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync } from "node:fs";
import { connect } from "node:net";
import { join } from "node:path";
import { Phases } from "../src/constants";
import { TypesenseProcessManager, type MultiNodeConfig } from "../src/manager";

setDefaultTimeout(5 * 60 * 1000);

const BASE_DIR = join(process.cwd(), "./data/join-snapshot");
// every wait below is for seconds of work on this data; the bound only ends a stuck cluster
const WAIT_TIMEOUT_MS = 2 * 60 * 1000;
// the server hands a streamed body to its handler in pieces of this many bytes (ACTIVE_STREAM_WINDOW_SIZE)
const STREAM_WINDOW_BYTES = 196_605;

function parsePositiveInt(name: string, defaultValue: number): number {
  const rawValue = process.env[name];
  if (rawValue === undefined || rawValue === "") {
    return defaultValue;
  }

  const parsedValue = Number.parseInt(rawValue, 10);
  if (!Number.isFinite(parsedValue) || parsedValue <= 0) {
    throw new Error(`Environment variable ${name} must be a positive integer. Received: ${rawValue}`);
  }

  return parsedValue;
}

const IMPORT_BATCH_SIZE = parsePositiveInt("JOIN_SNAPSHOT_IMPORT_BATCH_SIZE", 40_000);
const CATEGORY_COUNT_PER_MARKET = parsePositiveInt("JOIN_SNAPSHOT_CATEGORY_COUNT_PER_MARKET", 50);
const PRODUCT_COUNT_PER_MARKET = parsePositiveInt("JOIN_SNAPSHOT_PRODUCT_COUNT_PER_MARKET", 1_000);
const VEHICLE_COUNT_PER_MARKET = parsePositiveInt("JOIN_SNAPSHOT_VEHICLE_COUNT_PER_MARKET", 1_000);
const NON_TARGET_FITMENTS_PER_PRODUCT = parsePositiveInt("JOIN_SNAPSHOT_NON_TARGET_FITMENTS_PER_PRODUCT", 4);
const TARGET_PRODUCT_MATCH_COUNT = parsePositiveInt("JOIN_SNAPSHOT_TARGET_PRODUCT_MATCH_COUNT", 200);
const QUERY_PAGE_SIZE = parsePositiveInt("JOIN_SNAPSHOT_QUERY_PAGE_SIZE", 5);
// Tail fitments per market: an import held open after its first piece, and a complete one that arrives while the
// first is open. The held one's first part spans one to two STREAM_WINDOW_BYTES and the complete one less than one,
// so each is one raft entry.
const HELD_IMPORT_FIRST_PART_DOC_COUNT = parsePositiveInt("JOIN_SNAPSHOT_HELD_IMPORT_FIRST_PART_DOC_COUNT", 3_000);
const HELD_IMPORT_REST_DOC_COUNT = parsePositiveInt("JOIN_SNAPSHOT_HELD_IMPORT_REST_DOC_COUNT", 1_000);
const OVERLAPPING_IMPORT_DOC_COUNT = parsePositiveInt("JOIN_SNAPSHOT_OVERLAPPING_IMPORT_DOC_COUNT", 1_000);
const TARGET_PRODUCT_INDEX = Math.min(
  PRODUCT_COUNT_PER_MARKET - 1,
  Math.max(0, Math.floor(PRODUCT_COUNT_PER_MARKET / 2)),
);

if (VEHICLE_COUNT_PER_MARKET < TARGET_PRODUCT_MATCH_COUNT) {
  throw new Error(
    `JOIN_SNAPSHOT_VEHICLE_COUNT_PER_MARKET (${VEHICLE_COUNT_PER_MARKET}) must be >= ` +
    `JOIN_SNAPSHOT_TARGET_PRODUCT_MATCH_COUNT (${TARGET_PRODUCT_MATCH_COUNT}).`
  );
}

type MarketCode = "se" | "dk";

type MarketCollections = {
  vehicles: string;
  categories: string;
  products: string;
  fitments: string;
};

type MarketConfig = {
  code: MarketCode;
  seed: number;
  collections: MarketCollections;
  targetProductIndex: number;
  targetProductPid: string;
  targetCategoryId: string;
  fitmentCount: number;
  expectedVehicleIdSet: Set<string>;
};

function pad(value: number, width: number = 6): string {
  return `${value}`.padStart(width, "0");
}

function getCategoryId(code: MarketCode, index: number): string {
  return `${code}_category_${pad(index)}`;
}

function getProductPid(code: MarketCode, index: number): string {
  return `${code}_product_${pad(index)}`;
}

function getVariantPid(code: MarketCode, index: number): string {
  return `${code}_variant_${pad(index)}`;
}

function getVehicleId(code: MarketCode, index: number): string {
  return `${code}_vehicle_${pad(index)}`;
}

function getFitmentCountPerMarket(): number {
  return ((PRODUCT_COUNT_PER_MARKET - 1) * NON_TARGET_FITMENTS_PER_PRODUCT) + TARGET_PRODUCT_MATCH_COUNT;
}

function getTailFitmentCountPerMarket(): number {
  return HELD_IMPORT_FIRST_PART_DOC_COUNT + HELD_IMPORT_REST_DOC_COUNT + OVERLAPPING_IMPORT_DOC_COUNT;
}

function getExpectedProductCountPerMarket(): number {
  return PRODUCT_COUNT_PER_MARKET;
}

function getExpectedVehicleCountPerMarket(): number {
  return VEHICLE_COUNT_PER_MARKET;
}

function getExpectedFitmentCountPerMarket(market: MarketConfig): number {
  return market.fitmentCount + getTailFitmentCountPerMarket();
}

function buildMarketConfig(code: MarketCode, seed: number): MarketConfig {
  const collections = {
    vehicles: `vehicles_${code}`,
    categories: `categories_${code}`,
    products: `products_${code}`,
    fitments: `product_vehicle_fitments_${code}`,
  };

  return {
    code,
    seed,
    collections,
    targetProductIndex: TARGET_PRODUCT_INDEX,
    targetProductPid: getProductPid(code, TARGET_PRODUCT_INDEX),
    targetCategoryId: getCategoryId(code, TARGET_PRODUCT_INDEX % CATEGORY_COUNT_PER_MARKET),
    fitmentCount: getFitmentCountPerMarket(),
    expectedVehicleIdSet: new Set(
      Array.from({ length: TARGET_PRODUCT_MATCH_COUNT }, (_, index) => getVehicleId(code, index))
    ),
  };
}

const MARKETS: MarketConfig[] = [
  buildMarketConfig("se", 17),
  buildMarketConfig("dk", 43),
];

const CLUSTER_NODES: MultiNodeConfig[] = [
  {
    name: "join-snapshot-node1",
    port: 15108,
    peerPort: 15107,
    dataDir: "typesense-data-1",
    logDir: "typesense-1",
    analyticsDir: "typesense-data-1/analytics_db",
  },
  {
    name: "join-snapshot-node2",
    port: 16108,
    peerPort: 16107,
    dataDir: "typesense-data-2",
    logDir: "typesense-2",
    analyticsDir: "typesense-data-2/analytics_db",
  },
  {
    name: "join-snapshot-node3",
    port: 17108,
    peerPort: 17107,
    dataDir: "typesense-data-3",
    logDir: "typesense-3",
    analyticsDir: "typesense-data-3/analytics_db",
  },
];

type StatusResponse = {
  state: string;
  last_index: number;
  committed_index: number;
  known_applied_index: number;
  applying_index: number;
  queued_writes: number;
};

type HealthResponse = {
  ok: boolean;
};

type EndpointSnapshot = {
  status?: number;
  data?: unknown;
  error?: string;
};

type NodeDiagnosticsSnapshot = {
  name: string;
  port: number;
  health: EndpointSnapshot;
  stats: EndpointSnapshot;
};

type CollectionFieldSchema = {
  name: string;
  reference?: string;
};

type CollectionSchemaResponse = {
  name: string;
  num_documents?: number;
  fields: CollectionFieldSchema[];
};

type SearchHit = {
  document?: Record<string, unknown>;
};

type MultiSearchResponse = {
  results?: Array<{
    found?: number;
    hits?: SearchHit[];
  }>;
};

type SearchExecution = {
  status: number;
  ok: boolean;
  bodyText: string;
  bodyJson?: MultiSearchResponse;
};

let manager: TypesenseProcessManager | null = null;

function getNodeByName(name: string): MultiNodeConfig {
  const node = CLUSTER_NODES.find((entry) => entry.name === name);
  if (!node) {
    throw new Error(`Unknown node ${name}`);
  }
  return node;
}

function getNodeDataDir(node: MultiNodeConfig): string {
  return join(BASE_DIR, node.dataDir);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeJoinedRecords(value: unknown): Array<Record<string, unknown>> {
  if (Array.isArray(value)) {
    return value.filter(isRecord);
  }

  if (isRecord(value)) {
    return [value];
  }

  return [];
}

async function fetchNode(port: number, path: string, init: RequestInit = {}, timeoutMs: number = 30_000): Promise<Response> {
  return fetch(`http://localhost:${port}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      "X-TYPESENSE-API-KEY": "xyz",
      ...init.headers,
    },
    signal: AbortSignal.timeout(timeoutMs),
  });
}

async function getStatus(node: MultiNodeConfig): Promise<StatusResponse> {
  const res = await fetchNode(node.port, "/status", {}, 10_000);
  if (!res.ok) {
    throw new Error(`Unable to fetch /status from ${node.name}: ${res.status}`);
  }
  return await res.json() as StatusResponse;
}

async function getHealth(node: MultiNodeConfig): Promise<{ status: number; data: HealthResponse }> {
  const res = await fetchNode(node.port, "/health", {}, 10_000);
  const data = await res.json() as HealthResponse;
  return { status: res.status, data };
}

async function fetchEndpointSnapshot(port: number, path: string, timeoutMs: number = 10_000): Promise<EndpointSnapshot> {
  try {
    const res = await fetch(`http://localhost:${port}${path}`, {
      headers: {
        "X-TYPESENSE-API-KEY": "xyz",
      },
      signal: AbortSignal.timeout(timeoutMs),
    });

    const text = await res.text();
    let data: unknown = text;

    try {
      data = JSON.parse(text);
    } catch {}

    return {
      status: res.status,
      data,
    };
  } catch (error) {
    return {
      error: `${error}`,
    };
  }
}

async function pollClusterHealthAndStats(nodes: MultiNodeConfig[], label: string): Promise<NodeDiagnosticsSnapshot[]> {
  const snapshots = await Promise.all(nodes.map(async (node) => ({
    name: node.name,
    port: node.port,
    health: await fetchEndpointSnapshot(node.port, "/health"),
    stats: await fetchEndpointSnapshot(node.port, "/stats.json"),
  })));

  return snapshots;
}

async function waitForCondition(
  label: string,
  predicate: () => Promise<boolean>,
  timeoutMs: number,
  intervalMs: number = 1_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return;
    }
    await Bun.sleep(intervalMs);
  }
  throw new Error(`Timed out after ${timeoutMs} ms waiting for ${label}`);
}

async function waitForHealthy(node: MultiNodeConfig, timeoutMs: number): Promise<{ status: number; data: HealthResponse }> {
  let lastHealth: { status: number; data: HealthResponse } | null = null;
  await waitForCondition(`${node.name} health`, async () => {
    try {
      lastHealth = await getHealth(node);
      return lastHealth.status === 200 && lastHealth.data.ok === true;
    } catch {
      return false;
    }
  }, timeoutMs);

  if (!lastHealth) {
    throw new Error(`Health response missing for ${node.name}`);
  }

  return lastHealth;
}

function getAppliedIndex(status: StatusResponse): number {
  return status.applying_index === 0 ? status.known_applied_index : status.applying_index;
}

function hasRaftIndexParity(statuses: StatusResponse[]): boolean {
  const firstStatus = statuses[0];
  if (!firstStatus) {
    return false;
  }

  const firstCommittedIndex = firstStatus.committed_index;
  return statuses.every((status) =>
    status.last_index === status.committed_index &&
    status.committed_index === getAppliedIndex(status) &&
    status.committed_index === firstCommittedIndex
  );
}

async function waitForRaftIndexParity(nodes: MultiNodeConfig[], timeoutMs: number): Promise<Array<{ name: string; status?: StatusResponse; error?: string }>> {
  let lastStatuses: Array<{ name: string; status?: StatusResponse; error?: string }> = [];

  await waitForCondition("raft indexes to converge", async () => {
    lastStatuses = await Promise.all(nodes.map(async (node) => {
      try {
        return { name: node.name, status: await getStatus(node) };
      } catch (error) {
        return { name: node.name, error: `${error}` };
      }
    }));

    const statuses = lastStatuses
      .map((entry) => entry.status)
      .filter((entry): entry is StatusResponse => entry !== undefined);

    if (statuses.length !== nodes.length) {
      return false;
    }

    return hasRaftIndexParity(statuses);
  }, timeoutMs);

  return lastStatuses;
}

async function waitForDrainedWrites(nodes: MultiNodeConfig[], timeoutMs: number): Promise<Array<{ name: string; status?: StatusResponse; error?: string }>> {
  let lastStatuses: Array<{ name: string; status?: StatusResponse; error?: string }> = [];

  await waitForCondition("batched writes to drain", async () => {
    lastStatuses = await waitForRaftIndexParity(nodes, 10_000);
    const statuses = lastStatuses
      .map((entry) => entry.status)
      .filter((entry): entry is StatusResponse => entry !== undefined);

    if (statuses.length !== nodes.length) {
      return false;
    }

    return statuses.every((status) => status.queued_writes === 0);
  }, timeoutMs).catch((error) => {
    throw new Error(`${error.message}; last statuses: ${JSON.stringify(lastStatuses)}`);
  });

  return lastStatuses;
}

async function waitForAppliedIndexAtLeast(node: MultiNodeConfig, index: number, timeoutMs: number): Promise<StatusResponse> {
  let lastStatus: StatusResponse | null = null;
  await waitForCondition(`${node.name} to apply raft index ${index}`, async () => {
    try {
      lastStatus = await getStatus(node);
      return getAppliedIndex(lastStatus) >= index;
    } catch {
      return false;
    }
  }, timeoutMs, 50).catch((error) => {
    throw new Error(`${error.message}; last status: ${JSON.stringify(lastStatus)}`);
  });

  return lastStatus!;
}

function getTypesenseLogPath(node: MultiNodeConfig): string {
  return join(BASE_DIR, "logs", node.logDir, "typesense.log");
}

function getLogSize(node: MultiNodeConfig): number {
  try {
    return statSync(getTypesenseLogPath(node)).size;
  } catch {
    return 0;
  }
}

// Every node logs its raft configuration when a change commits; the nodes file is re-read every 10 s.
async function waitForConfigurationWithout(nodes: MultiNodeConfig[], removed: MultiNodeConfig, logSizes: number[],
                                           timeoutMs: number) {
  const removedPeer = `:${removed.peerPort}:${removed.port}`;
  await waitForCondition(`${nodes.map((node) => node.name).join(", ")} to commit a configuration without ${removed.name}`,
    async () => nodes.every((node, index) => {
      const newLines = readFileSync(getTypesenseLogPath(node)).subarray(logSizes[index]).toString("utf8").split("\n");
      return newLines.some((line) => line.includes("Configuration of this group is") && !line.includes(removedPeer));
    }), timeoutMs, 250);
}

function seedLocalSnapshotFromLeader(leaderNode: MultiNodeConfig, followerNode: MultiNodeConfig) {
  const leaderStateDir = join(getNodeDataDir(leaderNode), "state");
  const leaderSnapshotRoot = join(leaderStateDir, "snapshot");
  const snapshotEntries = readdirSync(leaderSnapshotRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && entry.name.startsWith("snapshot_"));

  if (snapshotEntries.length === 0) {
    throw new Error(`No snapshot directory found for ${leaderNode.name}`);
  }

  const followerStateDir = join(getNodeDataDir(followerNode), "state");
  mkdirSync(followerStateDir, { recursive: true });
  cpSync(join(leaderStateDir, "snapshot"), join(followerStateDir, "snapshot"), { recursive: true });
  cpSync(join(leaderStateDir, "meta"), join(followerStateDir, "meta"), { recursive: true });
}

// The count of the first "Serialized/Restored N in-flight requests" line the node logged past `fromOffset`.
function getLoggedInFlightRequestCount(node: MultiNodeConfig, verb: "Serialized" | "Restored", fromOffset: number): number | null {
  try {
    const logText = readFileSync(getTypesenseLogPath(node)).subarray(fromOffset).toString("utf8");
    const logMessage = verb === "Serialized" ? "for snapshot" : "from snapshot";
    const match = logText.match(new RegExp(`${verb} (\\d+) in-flight requests ${logMessage}\\.`));
    return match ? Number.parseInt(match[1] ?? "0", 10) : null;
  } catch {
    return null;
  }
}

async function waitForInFlightSnapshotLog(node: MultiNodeConfig, verb: "Serialized" | "Restored", fromOffset: number,
                                          timeoutMs: number): Promise<number> {
  let count: number | null = null;
  await waitForCondition(`${node.name} to log ${verb.toLowerCase()} in-flight snapshot requests`, async () => {
    count = getLoggedInFlightRequestCount(node, verb, fromOffset);
    return count !== null;
  }, timeoutMs, 250);

  return count!;
}

async function waitForLeader(nodes: MultiNodeConfig[], timeoutMs: number = 120_000): Promise<MultiNodeConfig> {
  let leader: MultiNodeConfig | null = null;

  await waitForCondition("leader election", async () => {
    try {
      const statuses = await Promise.all(nodes.map(async (node) => ({
        node,
        status: await getStatus(node),
      })));
      const leaders = statuses.filter(({ status }) => status.state.toUpperCase().includes("LEADER"));
      const electedLeader = leaders[0];
      if (leaders.length === 1 && electedLeader) {
        leader = electedLeader.node;
        return true;
      }
    } catch {}
    return false;
  }, timeoutMs);

  if (!leader) {
    throw new Error("Leader not found");
  }

  return leader;
}

function assertImportSucceeded(responseText: string, label: string) {
  if (!responseText.includes('"success":false')) {
    return;
  }

  const failedLines = responseText
    .split("\n")
    .filter((line) => line.includes('"success":false'))
    .slice(0, 3);

  throw new Error(`${label} import failed: ${failedLines.join("\n")}`);
}

async function importJsonlBatch(port: number, collection: string, lines: string[], label: string) {
  const res = await fetchNode(
    port,
    `/collections/${collection}/documents/import?action=create`,
    {
      method: "POST",
      headers: {
        "Content-Type": "text/plain",
      },
      body: lines.join("\n"),
    },
    WAIT_TIMEOUT_MS,
  );

  expect(res.ok).toBe(true);
  assertImportSucceeded(await res.text(), label);
}

async function importGeneratedDocuments(
  port: number,
  collection: string,
  totalDocuments: number,
  buildDocument: (index: number) => Record<string, unknown>,
) {
  for (let start = 0; start < totalDocuments; start += IMPORT_BATCH_SIZE) {
    const end = Math.min(start + IMPORT_BATCH_SIZE, totalDocuments);
    const lines: string[] = [];

    for (let index = start; index < end; index++) {
      lines.push(JSON.stringify(buildDocument(index)));
    }

    await importJsonlBatch(port, collection, lines, `${collection} ${start}-${end - 1}`);
  }
}

function buildCategoryDocument(market: MarketConfig, index: number): Record<string, unknown> {
  return {
    id: `${market.code}_category_doc_${pad(index)}`,
    category_id: getCategoryId(market.code, index),
    name: `${market.code.toUpperCase()} Category ${index}`,
  };
}

function buildProductDocument(market: MarketConfig, index: number): Record<string, unknown> {
  return {
    id: `${market.code}_product_doc_${pad(index)}`,
    product_pid: getProductPid(market.code, index),
    variant_pid: getVariantPid(market.code, index),
    primary_level_3_category_id: getCategoryId(market.code, index % CATEGORY_COUNT_PER_MARKET),
    title: `${market.code.toUpperCase()} Product ${index}`,
  };
}

function buildVehicleDocument(market: MarketConfig, index: number): Record<string, unknown> {
  return {
    id: `${market.code}_vehicle_doc_${pad(index)}`,
    vehicle_id: getVehicleId(market.code, index),
    manufacturer: index % 5 === 0 ? "Saab" : "Volvo",
    model: `${market.code.toUpperCase()} Model ${index}`,
  };
}

function getVehicleIndexForNonTargetFitment(market: MarketConfig, productIndex: number, fitmentIndex: number): number {
  return ((productIndex * 131) + (fitmentIndex * 17) + market.seed) % VEHICLE_COUNT_PER_MARKET;
}

function buildFitmentDocument(
  market: MarketConfig,
  fitmentSequence: number,
  productIndex: number,
  fitmentIndex: number,
): Record<string, unknown> {
  const vehicleIndex = productIndex === market.targetProductIndex
    ? fitmentIndex
    : getVehicleIndexForNonTargetFitment(market, productIndex, fitmentIndex);

  return {
    id: `${market.code}_fitment_doc_${pad(fitmentSequence)}`,
    variant_pid: getVariantPid(market.code, productIndex),
    vehicle_id: getVehicleId(market.code, vehicleIndex),
  };
}

async function importFitmentDocuments(port: number, market: MarketConfig) {
  let fitmentSequence = 0;
  let batchStart = 0;
  let lines: string[] = [];

  const flush = async () => {
    if (lines.length === 0) {
      return;
    }

    const batchEnd = fitmentSequence - 1;
    await importJsonlBatch(port, market.collections.fitments, lines, `${market.collections.fitments} ${batchStart}-${batchEnd}`);
    batchStart = fitmentSequence;
    lines = [];
  };

  for (let productIndex = 0; productIndex < PRODUCT_COUNT_PER_MARKET; productIndex++) {
    const fitmentsForProduct = productIndex === market.targetProductIndex
      ? TARGET_PRODUCT_MATCH_COUNT
      : NON_TARGET_FITMENTS_PER_PRODUCT;

    for (let fitmentIndex = 0; fitmentIndex < fitmentsForProduct; fitmentIndex++) {
      lines.push(JSON.stringify(buildFitmentDocument(market, fitmentSequence, productIndex, fitmentIndex)));
      fitmentSequence++;

      if (lines.length === IMPORT_BATCH_SIZE) {
        await flush();
      }
    }
  }

  await flush();
  expect(fitmentSequence).toBe(market.fitmentCount);
}

async function createCollection(port: number, schema: Record<string, unknown>) {
  const res = await fetchNode(port, "/collections", {
    method: "POST",
    body: JSON.stringify(schema),
  }, 120_000);

  expect(res.ok).toBe(true);
}

async function createJoinCollections(port: number) {
  for (const market of MARKETS) {
    await createCollection(port, {
      name: market.collections.vehicles,
      fields: [
        { name: "id", type: "string" },
        { name: "vehicle_id", type: "string" },
        { name: "manufacturer", type: "string" },
        { name: "model", type: "string" },
      ],
    });

    await createCollection(port, {
      name: market.collections.categories,
      fields: [
        { name: "id", type: "string" },
        { name: "category_id", type: "string" },
        { name: "name", type: "string" },
      ],
    });

    await createCollection(port, {
      name: market.collections.products,
      fields: [
        { name: "id", type: "string" },
        { name: "product_pid", type: "string" },
        { name: "variant_pid", type: "string" },
        { name: "primary_level_3_category_id", type: "string", reference: `${market.collections.categories}.category_id` },
        { name: "title", type: "string" },
      ],
    });

    await createCollection(port, {
      name: market.collections.fitments,
      fields: [
        { name: "id", type: "string" },
        { name: "variant_pid", type: "string", reference: `${market.collections.products}.variant_pid` },
        { name: "vehicle_id", type: "string", reference: `${market.collections.vehicles}.vehicle_id` },
      ],
    });
  }
}

async function importMarketData(port: number, market: MarketConfig) {
  await importGeneratedDocuments(
    port,
    market.collections.categories,
    CATEGORY_COUNT_PER_MARKET,
    (index) => buildCategoryDocument(market, index),
  );

  await importGeneratedDocuments(
    port,
    market.collections.products,
    PRODUCT_COUNT_PER_MARKET,
    (index) => buildProductDocument(market, index),
  );

  await importGeneratedDocuments(
    port,
    market.collections.vehicles,
    VEHICLE_COUNT_PER_MARKET,
    (index) => buildVehicleDocument(market, index),
  );

  await importFitmentDocuments(port, market);
}

function getExistingNonTargetProductIndex(market: MarketConfig, ordinal: number): number {
  const candidate = ordinal % (PRODUCT_COUNT_PER_MARKET - 1);
  return candidate >= market.targetProductIndex ? candidate + 1 : candidate;
}

function buildTailFitmentDocument(market: MarketConfig, fitmentSequence: number): Record<string, unknown> {
  const productIndex = getExistingNonTargetProductIndex(market, fitmentSequence + market.seed);
  const vehicleIndex = ((fitmentSequence * 17) + market.seed) % VEHICLE_COUNT_PER_MARKET;
  return {
    id: `${market.code}_fitment_doc_${pad(market.fitmentCount + fitmentSequence)}`,
    variant_pid: getVariantPid(market.code, productIndex),
    vehicle_id: getVehicleId(market.code, vehicleIndex),
  };
}

type HeldImport = {
  // sends the rest of the body and resolves with the raw HTTP response
  finish: () => Promise<string>;
  // the connection's state, for a failure message
  describe: () => string;
  // settles when the server closes the connection, which before finish() means it answered early
  closed: Promise<void>;
};

function chunked(text: string): string {
  return `${Buffer.byteLength(text).toString(16)}\r\n${text}\r\n`;
}

// Starts an import whose body is sent as chunked HTTP, and stops after the first part: the server replicates the
// request piece by piece, so until finish() it is an incomplete in-flight request.
async function startHeldImport(port: number, collection: string, firstPart: string, rest: string): Promise<HeldImport> {
  const socket = connect(port, "127.0.0.1");
  let response = "";
  socket.setEncoding("utf8");
  socket.on("data", (data) => { response += data; });
  const closed = new Promise<void>((resolve, reject) => {
    socket.on("close", () => resolve());
    socket.on("error", reject);
  });
  await new Promise<void>((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", reject);
  });
  socket.write(
    `POST /collections/${collection}/documents/import?action=create HTTP/1.1\r\n` +
    `Host: localhost:${port}\r\nX-TYPESENSE-API-KEY: xyz\r\nContent-Type: text/plain\r\n` +
    `Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n` + chunked(firstPart),
  );

  let isClosed = false;
  closed.then(() => { isClosed = true; }, () => { isClosed = true; });

  return {
    closed,
    describe() {
      return `${collection} held import: ${socket.bytesWritten} bytes written, ${socket.writableLength} buffered, ` +
        `closed=${isClosed}, response so far=${JSON.stringify(response.slice(0, 300))}`;
    },
    async finish() {
      socket.write(chunked(rest) + "0\r\n\r\n");
      let timer: Timer | undefined;
      await Promise.race([
        closed,
        new Promise((_, reject) => {
          timer = setTimeout(() => reject(new Error(`${collection} held import got no response in ${WAIT_TIMEOUT_MS} ms`)),
            WAIT_TIMEOUT_MS);
        }),
      ]).finally(() => clearTimeout(timer));
      return response;
    },
  };
}

type OverlappingImports = {
  heldImports: HeldImport[];
  overlappingImports: Promise<void>[];
};

// The shape 3671101c fixed: per market, an import is held open after its first piece is applied, then a complete
// import into the same collection is applied. The earlier-started request is incomplete and will finish last. The
// complete one isn't awaited: a server that waits on the held request never answers it until that one finishes.
async function startOverlappingReferenceImports(leader: MultiNodeConfig): Promise<OverlappingImports> {
  let appliedIndex = getAppliedIndex(await getStatus(leader));
  const heldImports: HeldImport[] = [];
  for (const market of MARKETS) {
    const lines = (start: number, count: number) => Array.from({ length: count }, (_, index) =>
      JSON.stringify(buildTailFitmentDocument(market, start + index))).join("\n");
    const firstPart = `${lines(0, HELD_IMPORT_FIRST_PART_DOC_COUNT)}\n`;
    // between one and two windows, so the first part is exactly one raft entry
    expect(Buffer.byteLength(firstPart)).toBeGreaterThan(STREAM_WINDOW_BYTES);
    expect(Buffer.byteLength(firstPart)).toBeLessThan(2 * STREAM_WINDOW_BYTES);
    const rest = lines(HELD_IMPORT_FIRST_PART_DOC_COUNT, HELD_IMPORT_REST_DOC_COUNT);
    const held = await startHeldImport(leader.port, market.collections.fitments, firstPart, rest);
    heldImports.push(held);
    appliedIndex += 1;
    await Promise.race([
      waitForAppliedIndexAtLeast(leader, appliedIndex, WAIT_TIMEOUT_MS),
      held.closed.then(() => { throw new Error("the server answered before the body was finished"); }),
    ]).catch((error) => {
      throw new Error(`${error.message}; ${held.describe()}`);
    });
  }

  const overlappingImports: Promise<void>[] = [];
  for (const market of MARKETS) {
    const start = HELD_IMPORT_FIRST_PART_DOC_COUNT + HELD_IMPORT_REST_DOC_COUNT;
    const lines = Array.from({ length: OVERLAPPING_IMPORT_DOC_COUNT }, (_, index) =>
      JSON.stringify(buildTailFitmentDocument(market, start + index)));
    expect(Buffer.byteLength(lines.join("\n"))).toBeLessThan(STREAM_WINDOW_BYTES);
    overlappingImports.push(importJsonlBatch(leader.port, market.collections.fitments, lines,
      `${market.collections.fitments} overlapping`));
    appliedIndex += 1;
    await waitForAppliedIndexAtLeast(leader, appliedIndex, WAIT_TIMEOUT_MS);
  }

  return { heldImports, overlappingImports };
}

function assertHeldImportSucceeded(response: string, label: string) {
  expect(response.split("\r\n")[0]).toBe("HTTP/1.1 200 OK");
  assertImportSucceeded(response, label);
}

async function getCollectionSchema(port: number, collection: string): Promise<CollectionSchemaResponse> {
  const res = await fetchNode(port, `/collections/${collection}`, {}, 30_000);
  expect(res.ok).toBe(true);
  return await res.json() as CollectionSchemaResponse;
}

async function tryGetCollectionSchema(port: number, collection: string): Promise<CollectionSchemaResponse | null> {
  try {
    const res = await fetchNode(port, `/collections/${collection}`, {}, 30_000);
    if (!res.ok) {
      return null;
    }
    return await res.json() as CollectionSchemaResponse;
  } catch {
    return null;
  }
}

function assertReferenceField(schema: CollectionSchemaResponse, fieldName: string, expectedReference: string) {
  const field = schema.fields.find((entry) => entry.name === fieldName);
  expect(field).toBeDefined();
  expect(field?.reference).toBe(expectedReference);
}

async function executeReferenceJoinSearch(market: MarketConfig, node: MultiNodeConfig): Promise<SearchExecution> {
  const res = await fetchNode(node.port, "/multi_search", {
    method: "POST",
    body: JSON.stringify({
      searches: [
        {
          collection: market.collections.vehicles,
          q: "*",
          query_by: "manufacturer",
          per_page: QUERY_PAGE_SIZE,
          filter_by: `$${market.collections.fitments}($${market.collections.products}(product_pid:=${market.targetProductPid}))`,
          include_fields: [
            "vehicle_id",
            "manufacturer",
            `$${market.collections.fitments}(variant_pid,vehicle_id,$${market.collections.products}(product_pid,title,variant_pid,primary_level_3_category_id,$${market.collections.categories}(category_id,name)))`,
          ].join(","),
        },
      ],
    }),
  }, 120_000);

  const bodyText = await res.text();
  let bodyJson: MultiSearchResponse | undefined;

  try {
    bodyJson = JSON.parse(bodyText) as MultiSearchResponse;
  } catch {}

  return {
    status: res.status,
    ok: res.ok,
    bodyText,
    bodyJson,
  };
}

function assertNestedJoinPayload(market: MarketConfig, searchBody: MultiSearchResponse) {
  const hits = searchBody.results?.[0]?.hits ?? [];

  const containsExpectedNestedProduct = hits.some((hit) => {
    const fitments = normalizeJoinedRecords(hit.document?.[market.collections.fitments]);
    return fitments.some((fitment) => {
      const products = normalizeJoinedRecords(fitment[market.collections.products]);
      return products.some((product) => {
        const categories = normalizeJoinedRecords(product[market.collections.categories]);
        return product.product_pid === market.targetProductPid &&
          categories.some((category) => category.category_id === market.targetCategoryId);
      });
    });
  });

  expect(containsExpectedNestedProduct).toBe(true);
}

async function assertJoinSearchSucceeds(market: MarketConfig, node: MultiNodeConfig) {
  const execution = await executeReferenceJoinSearch(market, node);

  if (!execution.ok || !execution.bodyJson) {
    const nodeStatus = await getStatus(node);
    throw new Error(
      `JOIN query failed for ${market.code.toUpperCase()} on ${node.name} (${node.port}) with HTTP ${execution.status}. ` +
      `raft_state=${nodeStatus.state}, committed_index=${nodeStatus.committed_index}, ` +
      `queued_writes=${nodeStatus.queued_writes}. body=${execution.bodyText}`
    );
  }

  const searchBody = execution.bodyJson;
  expect(searchBody.results?.[0]?.found).toBe(TARGET_PRODUCT_MATCH_COUNT);

  const hits = searchBody.results?.[0]?.hits ?? [];
  expect(hits.length).toBeGreaterThan(0);
  expect(hits.length).toBeLessThanOrEqual(QUERY_PAGE_SIZE);

  for (const hit of hits) {
    const vehicleId = hit.document?.vehicle_id;
    expect(typeof vehicleId).toBe("string");
    expect(market.expectedVehicleIdSet.has(vehicleId as string)).toBe(true);
  }

  assertNestedJoinPayload(market, searchBody);
}

async function assertAllJoinQueriesSucceed(nodes: MultiNodeConfig[]) {
  await Promise.all(
    MARKETS.flatMap((market) => nodes.map((node) => assertJoinSearchSucceeds(market, node)))
  );
}

function hasReferenceField(schema: CollectionSchemaResponse, fieldName: string, expectedReference: string): boolean {
  const field = schema.fields.find((entry) => entry.name === fieldName);
  return field?.reference === expectedReference;
}

async function waitForRestoredCollectionsToLoad(node: MultiNodeConfig, timeoutMs: number) {
  await waitForCondition("restored reference collections to finish loading", async () => {
    for (const market of MARKETS) {
      const [fitmentSchema, productSchema, vehicleSchema, categorySchema] = await Promise.all([
        tryGetCollectionSchema(node.port, market.collections.fitments),
        tryGetCollectionSchema(node.port, market.collections.products),
        tryGetCollectionSchema(node.port, market.collections.vehicles),
        tryGetCollectionSchema(node.port, market.collections.categories),
      ]);

      if (!fitmentSchema || !productSchema || !vehicleSchema || !categorySchema) {
        return false;
      }

      if (
        fitmentSchema.num_documents !== getExpectedFitmentCountPerMarket(market) ||
        productSchema.num_documents !== getExpectedProductCountPerMarket() ||
        vehicleSchema.num_documents !== getExpectedVehicleCountPerMarket() ||
        categorySchema.num_documents !== CATEGORY_COUNT_PER_MARKET
      ) {
        return false;
      }

      if (
        !hasReferenceField(fitmentSchema, "variant_pid", `${market.collections.products}.variant_pid`) ||
        !hasReferenceField(fitmentSchema, "vehicle_id", `${market.collections.vehicles}.vehicle_id`) ||
        !hasReferenceField(productSchema, "primary_level_3_category_id", `${market.collections.categories}.category_id`)
      ) {
        return false;
      }
    }

    return true;
  }, timeoutMs, 250);
}

async function assertRestoredReferenceSchemas(node: MultiNodeConfig) {
  for (const market of MARKETS) {
    const restoredFitmentSchema = await getCollectionSchema(node.port, market.collections.fitments);
    expect(restoredFitmentSchema.name).toBe(market.collections.fitments);
    expect(restoredFitmentSchema.num_documents).toBe(getExpectedFitmentCountPerMarket(market));
    assertReferenceField(restoredFitmentSchema, "variant_pid", `${market.collections.products}.variant_pid`);
    assertReferenceField(restoredFitmentSchema, "vehicle_id", `${market.collections.vehicles}.vehicle_id`);

    const restoredProductSchema = await getCollectionSchema(node.port, market.collections.products);
    expect(restoredProductSchema.name).toBe(market.collections.products);
    expect(restoredProductSchema.num_documents).toBe(getExpectedProductCountPerMarket());
    assertReferenceField(
      restoredProductSchema,
      "primary_level_3_category_id",
      `${market.collections.categories}.category_id`,
    );
  }
}

describe(Phases.NO_PHASE, () => {
  // inside the phase, so other phases' processes that load this file never touch its cluster
  beforeAll(() => {
    rmSync(BASE_DIR, { recursive: true, force: true });
    mkdirSync(BASE_DIR, { recursive: true });
    manager = new TypesenseProcessManager(BASE_DIR);
  });

  // the next run's beforeAll clears the directory; keeping it leaves the logs for a failed run
  afterAll(async () => {
    if (manager) {
      await manager.shutdown();
    }
  });

  it("preserves scaled reverse nested joins on a follower restarted from a leader snapshot with in-flight batched writes", async () => {
    if (!manager) {
      throw new Error("Process manager was not initialized");
    }
    const processManager = manager;

    await processManager.startMultiNode(CLUSTER_NODES);

    const initialLeader = await waitForLeader(CLUSTER_NODES);
    await createJoinCollections(initialLeader.port);

    for (const market of MARKETS) {
      await importMarketData(initialLeader.port, market);
    }

    await waitForDrainedWrites(CLUSTER_NODES, WAIT_TIMEOUT_MS);
    await assertAllJoinQueriesSucceed(CLUSTER_NODES);

    const leaderBeforeRemoval = await waitForLeader(CLUSTER_NODES);
    const followerToRestore = CLUSTER_NODES.find((node) => node.name !== leaderBeforeRemoval.name);

    if (!followerToRestore) {
      throw new Error("Follower node not found");
    }

    const remainingNodes = CLUSTER_NODES.filter((node) => node.name !== followerToRestore.name);

    await processManager.stopServer(followerToRestore.name);
    const logSizesBeforeRemoval = remainingNodes.map(getLogSize);
    processManager.writeNodesConfig(remainingNodes);

    const nodesConfigAfterRemoval = readFileSync(processManager.nodesFile, "utf8");
    expect(nodesConfigAfterRemoval).not.toContain(`:${followerToRestore.peerPort}:${followerToRestore.port}`);

    await waitForConfigurationWithout(remainingNodes, followerToRestore, logSizesBeforeRemoval, WAIT_TIMEOUT_MS);
    await pollClusterHealthAndStats(remainingNodes, "Cluster /health and /stats.json after the peer refresh");

    await Promise.all(remainingNodes.map((node) => waitForHealthy(node, WAIT_TIMEOUT_MS)));
    await waitForRaftIndexParity(remainingNodes, WAIT_TIMEOUT_MS);

    const leaderAfterRemoval = await waitForLeader(remainingNodes);
    const { heldImports, overlappingImports } = await startOverlappingReferenceImports(leaderAfterRemoval);

    const leaderLogSizeBeforeSnapshot = getLogSize(leaderAfterRemoval);
    const snapshotRes = await fetchNode(leaderAfterRemoval.port, "/operations/snapshot", {
      method: "POST",
    }, WAIT_TIMEOUT_MS);
    expect(snapshotRes.status).toBe(201);
    expect(await snapshotRes.json()).toEqual({ success: true });
    // at least each market's held import, still incomplete
    expect(await waitForInFlightSnapshotLog(leaderAfterRemoval, "Serialized", leaderLogSizeBeforeSnapshot, WAIT_TIMEOUT_MS))
      .toBeGreaterThanOrEqual(MARKETS.length);

    const heldResponses = await Promise.all(heldImports.map((held) => held.finish()));
    await waitForDrainedWrites(remainingNodes, WAIT_TIMEOUT_MS);
    heldResponses.forEach((response, index) =>
      assertHeldImportSucceeded(response, `${MARKETS[index]!.collections.fitments} held`));
    await Promise.all(overlappingImports);

    rmSync(join(BASE_DIR, followerToRestore.dataDir), { recursive: true, force: true });
    mkdirSync(join(BASE_DIR, followerToRestore.dataDir), { recursive: true });
    seedLocalSnapshotFromLeader(leaderAfterRemoval, followerToRestore);
    processManager.writeNodesConfig(CLUSTER_NODES);

    const nodesConfigAfterRestore = readFileSync(processManager.nodesFile, "utf8");
    expect(nodesConfigAfterRestore).toContain(`:${followerToRestore.peerPort}:${followerToRestore.port}`);

    const restoredLogSizeBeforeStart = getLogSize(followerToRestore);
    await processManager.startClusterNode(followerToRestore, processManager.nodesFile, false);

    const restoredNode = getNodeByName(followerToRestore.name);
    const restoredHealth = await waitForHealthy(restoredNode, WAIT_TIMEOUT_MS);
    expect(restoredHealth.status).toBe(200);
    expect(restoredHealth.data.ok).toBe(true);
    expect(await waitForInFlightSnapshotLog(restoredNode, "Restored", restoredLogSizeBeforeStart, WAIT_TIMEOUT_MS))
      .toBeGreaterThanOrEqual(MARKETS.length);

    await Promise.all(CLUSTER_NODES.map((node) => waitForHealthy(node, WAIT_TIMEOUT_MS)));
    await waitForDrainedWrites(CLUSTER_NODES, WAIT_TIMEOUT_MS);

    await waitForRestoredCollectionsToLoad(restoredNode, WAIT_TIMEOUT_MS);
    await assertRestoredReferenceSchemas(restoredNode);
    await assertAllJoinQueriesSucceed(CLUSTER_NODES);
  }, { timeout: 5 * 60 * 1000 });
});
